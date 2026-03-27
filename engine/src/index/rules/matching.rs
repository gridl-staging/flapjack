use super::*;
use std::collections::{HashMap, HashSet};

impl Rule {
    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    pub fn is_valid_at(&self, timestamp: i64) -> bool {
        match &self.validity {
            None => true,
            Some(ranges) => ranges
                .iter()
                .any(|r| timestamp >= r.from && timestamp <= r.until),
        }
    }

    /// Test whether this rule matches the given query, context, and active filters.
    ///
    /// Return `false` if the rule is disabled or outside its validity window. Return `true` if the rule has no conditions. Otherwise return `true` if any condition matches (OR logic).
    pub fn matches(
        &self,
        query_text: &str,
        context: Option<&[String]>,
        active_filters: Option<&Filter>,
        synonyms: Option<&SynonymStore>,
    ) -> bool {
        if !self.is_enabled() {
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        if !self.is_valid_at(now) {
            return false;
        }

        if self.conditions.is_empty() {
            return true;
        }

        for condition in &self.conditions {
            if self.matches_condition(query_text, condition, context, active_filters, synonyms) {
                return true;
            }
        }

        false
    }

    /// After confirming a rule matches, extract `{facet:attrName}` captures from the
    /// first matching condition. Returns `{attr_name → captured_word}`.
    pub(super) fn extract_matching_facet_captures(
        &self,
        query_text: &str,
        context: Option<&[String]>,
        active_filters: Option<&Filter>,
        synonyms: Option<&SynonymStore>,
    ) -> HashMap<String, String> {
        for condition in &self.conditions {
            if self.matches_condition(query_text, condition, context, active_filters, synonyms) {
                if let Some(pattern) = &condition.pattern {
                    if pattern.contains("{facet:") {
                        return extract_facet_captures(
                            query_text,
                            pattern,
                            condition.anchoring.as_ref(),
                        );
                    }
                }
            }
        }
        HashMap::new()
    }

    /// TODO: Document Rule.matches_condition.
    fn matches_condition(
        &self,
        query_text: &str,
        condition: &Condition,
        context: Option<&[String]>,
        active_filters: Option<&Filter>,
        synonyms: Option<&SynonymStore>,
    ) -> bool {
        if let Some(ctx) = &condition.context {
            let context_matches = context
                .is_some_and(|contexts| contexts.iter().any(|rule_context| rule_context == ctx));
            if !context_matches {
                return false;
            }
        }

        if let Some(pattern) = condition.pattern.as_deref() {
            if !self.matches_pattern_with_alternatives(
                query_text,
                pattern,
                condition.anchoring.as_ref(),
                condition.alternatives,
                synonyms,
            ) {
                return false;
            }
        }

        if let Some(condition_filters) = condition.filters.as_deref() {
            if !self.matches_condition_filters(condition_filters, active_filters) {
                return false;
            }
        }

        true
    }

    /// TODO: Document Rule.matches_pattern.
    fn matches_pattern(
        &self,
        query_text: &str,
        pattern: &str,
        anchoring: Option<&Anchoring>,
    ) -> bool {
        if pattern.contains("{facet:") {
            let pattern_tokens = parse_pattern_tokens(pattern);
            let query_tokens = tokenize_for_rule_matching(query_text);
            return match_pattern_tokens_with_placeholders(
                &query_tokens,
                &pattern_tokens,
                anchoring.unwrap_or(&Anchoring::Contains),
            );
        }

        let query_tokens = tokenize_for_rule_matching(query_text);
        let pattern_tokens = tokenize_for_rule_matching(pattern);
        let anchoring = anchoring.unwrap_or(&Anchoring::Contains);

        if pattern_tokens.is_empty() {
            return match anchoring {
                Anchoring::Is => query_tokens.is_empty(),
                Anchoring::StartsWith | Anchoring::EndsWith | Anchoring::Contains => true,
            };
        }

        match anchoring {
            Anchoring::Is => query_tokens == pattern_tokens,
            Anchoring::StartsWith => {
                query_tokens.len() >= pattern_tokens.len()
                    && query_tokens
                        .iter()
                        .zip(pattern_tokens.iter())
                        .all(|(q, p)| q == p)
            }
            Anchoring::EndsWith => {
                if query_tokens.len() < pattern_tokens.len() {
                    return false;
                }
                let offset = query_tokens.len() - pattern_tokens.len();
                query_tokens[offset..]
                    .iter()
                    .zip(pattern_tokens.iter())
                    .all(|(q, p)| q == p)
            }
            Anchoring::Contains => {
                if query_tokens.len() < pattern_tokens.len() {
                    return false;
                }
                query_tokens.windows(pattern_tokens.len()).any(|window| {
                    window
                        .iter()
                        .zip(pattern_tokens.iter())
                        .all(|(q, p)| q == p)
                })
            }
        }
    }

    /// TODO: Document Rule.matches_pattern_with_alternatives.
    fn matches_pattern_with_alternatives(
        &self,
        query_text: &str,
        pattern: &str,
        anchoring: Option<&Anchoring>,
        alternatives: Option<bool>,
        synonyms: Option<&SynonymStore>,
    ) -> bool {
        if self.matches_pattern(query_text, pattern, anchoring) {
            return true;
        }

        if alternatives != Some(true) {
            return false;
        }

        if let Some(syn_store) = synonyms {
            let expansions = syn_store.expand_query(pattern);
            for expansion in &expansions {
                if expansion != pattern && self.matches_pattern(query_text, expansion, anchoring) {
                    return true;
                }
            }
        }

        let pattern_words = tokenize_for_rule_matching(pattern);
        let query_words = tokenize_for_rule_matching(query_text);

        let anchoring = anchoring.unwrap_or(&Anchoring::Contains);
        match anchoring {
            Anchoring::Is => {
                if pattern_words.len() != query_words.len() {
                    return false;
                }
                pattern_words
                    .iter()
                    .zip(query_words.iter())
                    .all(|(pw, qw)| fuzzy_word_match(pw, qw))
            }
            Anchoring::Contains => {
                if pattern_words.len() > query_words.len() {
                    return false;
                }
                if pattern_words.len() == 1 {
                    query_words
                        .iter()
                        .any(|qw| fuzzy_word_match(&pattern_words[0], qw))
                } else {
                    query_words.windows(pattern_words.len()).any(|window| {
                        window
                            .iter()
                            .zip(pattern_words.iter())
                            .all(|(qw, pw)| fuzzy_word_match(pw, qw))
                    })
                }
            }
            Anchoring::StartsWith => {
                if pattern_words.len() > query_words.len() {
                    return false;
                }
                pattern_words
                    .iter()
                    .zip(query_words.iter())
                    .all(|(pw, qw)| fuzzy_word_match(pw, qw))
            }
            Anchoring::EndsWith => {
                if pattern_words.len() > query_words.len() {
                    return false;
                }
                let offset = query_words.len() - pattern_words.len();
                pattern_words
                    .iter()
                    .zip(query_words[offset..].iter())
                    .all(|(pw, qw)| fuzzy_word_match(pw, qw))
            }
        }
    }

    fn matches_condition_filters(
        &self,
        condition_filters: &str,
        active_filters: Option<&Filter>,
    ) -> bool {
        let active = match active_filters {
            Some(f) => f,
            None => return false,
        };
        let condition_ast = match filter_parser::parse_filter(condition_filters) {
            Ok(f) => f,
            Err(_) => return false,
        };
        filter_parser::filter_implies(&condition_ast, active)
    }
}

/// Check if two words are a fuzzy match using Levenshtein distance.
pub(super) fn fuzzy_word_match(pattern_word: &str, query_word: &str) -> bool {
    let pw = pattern_word.to_lowercase();
    let qw = query_word.to_lowercase();
    if pw == qw {
        return true;
    }
    let max_distance = calculate_typo_distance(&pw);
    if max_distance == 0 {
        return false;
    }
    typo_distance(&pw, &qw) <= max_distance as usize
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PatternToken {
    Literal(String),
    FacetCapture(String),
}

/// TODO: Document parse_pattern_tokens.
pub(super) fn parse_pattern_tokens(pattern: &str) -> Vec<PatternToken> {
    let mut tokens = Vec::new();
    let mut remaining = pattern;

    while !remaining.is_empty() {
        if let Some(start) = remaining.find("{facet:") {
            let before = &remaining[..start];
            for tok in tokenize_for_rule_matching(before) {
                tokens.push(PatternToken::Literal(tok));
            }
            let after_open = &remaining[start + 7..];
            if let Some(end) = after_open.find('}') {
                let attr = after_open[..end].trim().to_string();
                if !attr.is_empty() {
                    tokens.push(PatternToken::FacetCapture(attr));
                }
                remaining = &after_open[end + 1..];
            } else {
                for tok in tokenize_for_rule_matching(remaining) {
                    tokens.push(PatternToken::Literal(tok));
                }
                break;
            }
        } else {
            for tok in tokenize_for_rule_matching(remaining) {
                tokens.push(PatternToken::Literal(tok));
            }
            break;
        }
    }

    tokens
}

/// TODO: Document match_pattern_tokens_with_placeholders.
pub(super) fn match_pattern_tokens_with_placeholders(
    query_tokens: &[String],
    pattern_tokens: &[PatternToken],
    anchoring: &Anchoring,
) -> bool {
    if pattern_tokens.is_empty() {
        return match anchoring {
            Anchoring::Is => query_tokens.is_empty(),
            _ => true,
        };
    }

    if query_tokens.len() < pattern_tokens.len() {
        return false;
    }

    let window_matches = |window: &[String]| -> bool {
        window
            .iter()
            .zip(pattern_tokens.iter())
            .all(|(q, p)| match p {
                PatternToken::Literal(lit) => q == lit,
                PatternToken::FacetCapture(_) => true,
            })
    };

    match anchoring {
        Anchoring::Is => query_tokens.len() == pattern_tokens.len() && window_matches(query_tokens),
        Anchoring::StartsWith => window_matches(&query_tokens[..pattern_tokens.len()]),
        Anchoring::EndsWith => {
            let offset = query_tokens.len() - pattern_tokens.len();
            window_matches(&query_tokens[offset..])
        }
        Anchoring::Contains => query_tokens
            .windows(pattern_tokens.len())
            .any(window_matches),
    }
}

/// TODO: Document extract_facet_captures.
pub(super) fn extract_facet_captures(
    query_text: &str,
    pattern: &str,
    anchoring: Option<&Anchoring>,
) -> HashMap<String, String> {
    let mut captures = HashMap::new();

    if !pattern.contains("{facet:") {
        return captures;
    }

    let pattern_tokens = parse_pattern_tokens(pattern);
    let query_tokens = tokenize_for_rule_matching(query_text);
    let anchoring = anchoring.unwrap_or(&Anchoring::Contains);

    if query_tokens.len() < pattern_tokens.len() {
        return captures;
    }

    let try_capture = |window: &[String], captures: &mut HashMap<String, String>| -> bool {
        let matched = window
            .iter()
            .zip(pattern_tokens.iter())
            .all(|(q, p)| match p {
                PatternToken::Literal(lit) => q == lit,
                PatternToken::FacetCapture(_) => true,
            });
        if matched {
            for (q, p) in window.iter().zip(pattern_tokens.iter()) {
                if let PatternToken::FacetCapture(attr) = p {
                    captures.insert(attr.clone(), q.clone());
                }
            }
        }
        matched
    };

    match anchoring {
        Anchoring::Is => {
            if query_tokens.len() == pattern_tokens.len() {
                try_capture(&query_tokens, &mut captures);
            }
        }
        Anchoring::StartsWith => {
            try_capture(&query_tokens[..pattern_tokens.len()], &mut captures);
        }
        Anchoring::EndsWith => {
            let offset = query_tokens.len() - pattern_tokens.len();
            try_capture(&query_tokens[offset..], &mut captures);
        }
        Anchoring::Contains => {
            for window in query_tokens.windows(pattern_tokens.len()) {
                if try_capture(window, &mut captures) {
                    break;
                }
            }
        }
    }

    captures
}

/// TODO: Document tokenize_for_rule_matching.
pub(super) fn tokenize_for_rule_matching(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        if ch.is_alphanumeric() {
            current.extend(ch.to_lowercase());
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn token_sequence_matches_at(tokens: &[String], target: &[String], start: usize) -> bool {
    if target.is_empty() || start + target.len() > tokens.len() {
        return false;
    }
    tokens[start..start + target.len()]
        .iter()
        .zip(target.iter())
        .all(|(token, target_token)| token == target_token)
}

/// TODO: Document remove_token_sequence.
fn remove_token_sequence(tokens: Vec<String>, target: &[String]) -> Vec<String> {
    if tokens.is_empty() || target.is_empty() {
        return tokens;
    }

    let mut rewritten = Vec::with_capacity(tokens.len());
    let mut cursor = 0usize;

    while cursor < tokens.len() {
        if token_sequence_matches_at(&tokens, target, cursor) {
            cursor += target.len();
            continue;
        }
        rewritten.push(tokens[cursor].clone());
        cursor += 1;
    }

    rewritten
}

/// TODO: Document replace_token_sequence.
fn replace_token_sequence(
    tokens: Vec<String>,
    target: &[String],
    insert: &[String],
) -> Vec<String> {
    if tokens.is_empty() || target.is_empty() {
        return tokens;
    }

    let mut rewritten = Vec::with_capacity(tokens.len() + insert.len());
    let mut cursor = 0usize;

    while cursor < tokens.len() {
        if token_sequence_matches_at(&tokens, target, cursor) {
            rewritten.extend(insert.iter().cloned());
            cursor += target.len();
            continue;
        }
        rewritten.push(tokens[cursor].clone());
        cursor += 1;
    }

    rewritten
}

/// TODO: Document apply_query_edits_to_text.
pub(super) fn apply_query_edits_to_text(
    query_text: &str,
    remove: Option<&[String]>,
    edits: Option<&[Edit]>,
) -> String {
    let mut remove_targets: Vec<Vec<String>> = Vec::new();
    if let Some(words) = remove {
        for word in words {
            let delete_tokens = tokenize_for_rule_matching(word);
            if !delete_tokens.is_empty() {
                remove_targets.push(delete_tokens);
            }
        }
    }

    let mut replace_targets: Vec<(Vec<String>, Vec<String>)> = Vec::new();
    if let Some(edit_specs) = edits {
        for edit in edit_specs {
            let delete_tokens = tokenize_for_rule_matching(&edit.delete);
            if delete_tokens.is_empty() {
                continue;
            }

            match edit.edit_type {
                EditType::Remove => remove_targets.push(delete_tokens),
                EditType::Replace => {
                    let insert_tokens = edit
                        .insert
                        .as_deref()
                        .map(tokenize_for_rule_matching)
                        .unwrap_or_default();
                    replace_targets.push((delete_tokens, insert_tokens));
                }
            }
        }
    }

    if remove_targets.is_empty() && replace_targets.is_empty() {
        return query_text.to_string();
    }

    let mut rewritten_tokens = tokenize_for_rule_matching(query_text);
    for delete_tokens in &remove_targets {
        rewritten_tokens = remove_token_sequence(rewritten_tokens, delete_tokens);
    }

    let removed_targets: HashSet<Vec<String>> = remove_targets.iter().cloned().collect();
    for (delete_tokens, insert_tokens) in replace_targets {
        if removed_targets.contains(&delete_tokens) {
            continue;
        }
        rewritten_tokens = replace_token_sequence(rewritten_tokens, &delete_tokens, &insert_tokens);
    }

    rewritten_tokens.join(" ")
}

pub(super) fn calculate_typo_distance(word: &str) -> u8 {
    let len = word.chars().count();
    if len < 5 {
        0
    } else if len < 9 {
        1
    } else {
        2
    }
}

fn typo_distance(a: &str, b: &str) -> usize {
    strsim::damerau_levenshtein(a, b)
}
