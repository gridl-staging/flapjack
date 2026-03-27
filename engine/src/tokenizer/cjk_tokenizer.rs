use crate::text_normalization::normalize_for_search;
use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

#[derive(Clone)]
pub struct CjkAwareTokenizer {
    /// When true, CJK characters are split into individual character tokens.
    /// When false, CJK characters are grouped into word tokens like Latin text.
    ///
    /// Character-level CJK splitting is the intentional launch behavior.
    /// See docs2/1_STRATEGY/LANGUAGE_SUPPORT_MATRIX.md for current limitations
    /// and the deferred ICU/MeCab-style upgrade path.
    cjk_splitting: bool,
    indexed_separators: Vec<char>,
    keep_diacritics_on_characters: String,
    custom_normalization: Vec<(char, String)>,
}

impl CjkAwareTokenizer {
    /// Create a tokenizer with CJK character-level splitting enabled (default).
    pub fn new() -> Self {
        Self {
            cjk_splitting: true,
            indexed_separators: Vec::new(),
            keep_diacritics_on_characters: String::new(),
            custom_normalization: Vec::new(),
        }
    }

    /// Create a tokenizer that treats CJK characters as regular word characters.
    /// Use this for indexes that only contain Latin-script languages.
    pub fn latin_only() -> Self {
        Self {
            cjk_splitting: false,
            indexed_separators: Vec::new(),
            keep_diacritics_on_characters: String::new(),
            custom_normalization: Vec::new(),
        }
    }

    /// Configure separator characters to emit as standalone tokens in addition to
    /// splitting tokens around them.
    pub fn with_indexed_separators(mut self, indexed_separators: Vec<char>) -> Self {
        self.indexed_separators = indexed_separators;
        self
    }

    pub fn with_keep_diacritics_on_characters(
        mut self,
        keep_diacritics_on_characters: &str,
    ) -> Self {
        self.keep_diacritics_on_characters = keep_diacritics_on_characters.to_string();
        self
    }

    pub fn with_custom_normalization(mut self, custom_normalization: Vec<(char, String)>) -> Self {
        self.custom_normalization = custom_normalization;
        self
    }
}

impl Default for CjkAwareTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a character is in a CJK Unicode range.
///
/// Matches Chinese ideographs, Japanese Hiragana and Katakana, Korean Hangul, and related symbol blocks.
fn is_cjk(c: char) -> bool {
    matches!(c,
        '\u{4E00}'..='\u{9FFF}' |
        '\u{3400}'..='\u{4DBF}' |
        '\u{F900}'..='\u{FAFF}' |
        '\u{2E80}'..='\u{2EFF}' |
        '\u{3000}'..='\u{303F}' |
        '\u{3040}'..='\u{309F}' |
        '\u{30A0}'..='\u{30FF}' |
        '\u{31F0}'..='\u{31FF}' |
        '\u{AC00}'..='\u{D7AF}' |
        '\u{1100}'..='\u{11FF}' |
        '\u{20000}'..='\u{2A6DF}' |
        '\u{2A700}'..='\u{2B73F}' |
        '\u{2B740}'..='\u{2B81F}' |
        '\u{2B820}'..='\u{2CEAF}'
    )
}

fn is_intra_word_separator(c: char) -> bool {
    !c.is_alphanumeric() && !c.is_whitespace() && !is_cjk(c) && c != '\0'
}

fn should_emit_concat_token(pending_parts: usize, concat_text: &str) -> bool {
    pending_parts >= 2 && concat_text.len() >= 3
}

/// TODO: Document push_normalized_token.
fn push_normalized_token(
    tokens: &mut Vec<Token>,
    position: &mut usize,
    raw_text: &str,
    offset_from: usize,
    offset_to: usize,
    keep_diacritics_on_characters: &str,
    custom_normalization: &[(char, String)],
) {
    let normalized = normalize_for_search(
        raw_text,
        keep_diacritics_on_characters,
        custom_normalization,
    );
    if normalized.is_empty() {
        return;
    }

    tokens.push(Token {
        offset_from,
        offset_to,
        position: *position,
        text: normalized,
        ..Default::default()
    });
    *position += 1;
}

/// TODO: Document flush_pending_concat_token.
fn flush_pending_concat_token(
    tokens: &mut Vec<Token>,
    position: &mut usize,
    pending_concat: &mut Option<(usize, String)>,
    pending_parts: usize,
    offset_to: usize,
    keep_diacritics_on_characters: &str,
    custom_normalization: &[(char, String)],
) {
    let Some((concat_start, concat_text)) = pending_concat.take() else {
        return;
    };
    if !should_emit_concat_token(pending_parts, &concat_text) {
        return;
    }

    push_normalized_token(
        tokens,
        position,
        &concat_text,
        concat_start,
        offset_to,
        keep_diacritics_on_characters,
        custom_normalization,
    );
}

/// TODO: Document read_alphanumeric_word.
fn read_alphanumeric_word(
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    cjk_splitting: bool,
) -> (usize, usize, String) {
    let (start, _) = chars.peek().copied().unwrap_or((0, '\0'));
    let mut end = start;
    let mut word = String::new();

    while let Some(&(byte_index, current_char)) = chars.peek() {
        if current_char.is_alphanumeric() && (!is_cjk(current_char) || !cjk_splitting) {
            end = byte_index + current_char.len_utf8();
            word.push(current_char);
            chars.next();
        } else {
            break;
        }
    }

    (start, end, word)
}

/// TODO: Document update_pending_concat.
fn update_pending_concat(
    pending_concat: &mut Option<(usize, String)>,
    pending_parts: &mut usize,
    saw_separator: bool,
    start: usize,
    word: &str,
) {
    if saw_separator {
        if let Some((_, concat_text)) = pending_concat {
            concat_text.push_str(word);
            *pending_parts += 1;
        }
        return;
    }

    if pending_concat.is_none() {
        *pending_concat = Some((start, word.to_string()));
        *pending_parts = 1;
    }
}

pub struct CjkAwareTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for CjkAwareTokenStream {
    fn advance(&mut self) -> bool {
        if self.index < self.tokens.len() {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

impl Tokenizer for CjkAwareTokenizer {
    type TokenStream<'a> = CjkAwareTokenStream;

    /// Tokenize the input text into a token stream.
    ///
    /// CJK characters are emitted individually when cjk_splitting is enabled (default). Latin text forms word tokens. Hyphenated or punctuation-separated sequences emit both individual parts and a concatenated token (when 3+ characters). Applies text normalization based on configuration.
    ///
    /// # Arguments
    ///
    /// * `text` - The string to tokenize
    ///
    /// # Returns
    ///
    /// A TokenStream advancing through tokens in position order.
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let keep_diacritics_on_characters = &self.keep_diacritics_on_characters;
        let custom_normalization = &self.custom_normalization;
        let mut tokens = Vec::new();
        let mut position = 0;
        let mut chars = text.char_indices().peekable();

        let mut pending_concat: Option<(usize, String)> = None;
        let mut pending_parts: usize = 0;
        let mut saw_separator = false;

        while let Some(&(byte_offset, c)) = chars.peek() {
            if self.cjk_splitting && is_cjk(c) {
                flush_pending_concat_token(
                    &mut tokens,
                    &mut position,
                    &mut pending_concat,
                    pending_parts,
                    byte_offset,
                    keep_diacritics_on_characters,
                    custom_normalization,
                );
                pending_parts = 0;
                saw_separator = false;

                let len = c.len_utf8();
                push_normalized_token(
                    &mut tokens,
                    &mut position,
                    &text[byte_offset..byte_offset + len],
                    byte_offset,
                    byte_offset + len,
                    keep_diacritics_on_characters,
                    custom_normalization,
                );
                chars.next();
                continue;
            }

            if c.is_alphanumeric() {
                let (start, end, word) = read_alphanumeric_word(&mut chars, self.cjk_splitting);
                push_normalized_token(
                    &mut tokens,
                    &mut position,
                    &word,
                    start,
                    end,
                    keep_diacritics_on_characters,
                    custom_normalization,
                );
                update_pending_concat(
                    &mut pending_concat,
                    &mut pending_parts,
                    saw_separator,
                    start,
                    &word,
                );
                saw_separator = false;
                continue;
            }

            if self.indexed_separators.contains(&c) {
                flush_pending_concat_token(
                    &mut tokens,
                    &mut position,
                    &mut pending_concat,
                    pending_parts,
                    byte_offset,
                    keep_diacritics_on_characters,
                    custom_normalization,
                );
                pending_parts = 0;
                saw_separator = false;

                let len = c.len_utf8();
                push_normalized_token(
                    &mut tokens,
                    &mut position,
                    &text[byte_offset..byte_offset + len],
                    byte_offset,
                    byte_offset + len,
                    keep_diacritics_on_characters,
                    custom_normalization,
                );
                chars.next();
                continue;
            }

            if is_intra_word_separator(c) {
                saw_separator = true;
                chars.next();
                continue;
            }

            flush_pending_concat_token(
                &mut tokens,
                &mut position,
                &mut pending_concat,
                pending_parts,
                byte_offset,
                keep_diacritics_on_characters,
                custom_normalization,
            );
            pending_parts = 0;
            saw_separator = false;
            chars.next();
        }

        flush_pending_concat_token(
            &mut tokens,
            &mut position,
            &mut pending_concat,
            pending_parts,
            text.len(),
            keep_diacritics_on_characters,
            custom_normalization,
        );

        CjkAwareTokenStream { tokens, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::tokenizer::Tokenizer;

    fn collect_tokens(text: &str) -> Vec<String> {
        let mut tokenizer = CjkAwareTokenizer::new();
        let mut stream = tokenizer.token_stream(text);
        let mut result = Vec::new();
        while stream.advance() {
            result.push(stream.token().text.clone());
        }
        result
    }

    fn collect_tokens_latin_only(text: &str) -> Vec<String> {
        let mut tokenizer = CjkAwareTokenizer::latin_only();
        let mut stream = tokenizer.token_stream(text);
        let mut result = Vec::new();
        while stream.advance() {
            result.push(stream.token().text.clone());
        }
        result
    }

    fn collect_tokens_with_indexed_separators(text: &str, separators: &[char]) -> Vec<String> {
        let mut tokenizer = CjkAwareTokenizer::new().with_indexed_separators(separators.to_vec());
        let mut stream = tokenizer.token_stream(text);
        let mut result = Vec::new();
        while stream.advance() {
            result.push(stream.token().text.clone());
        }
        result
    }

    // ── is_cjk ──────────────────────────────────────────────────────────

    #[test]
    fn is_cjk_chinese() {
        assert!(is_cjk('中'));
        assert!(is_cjk('国'));
    }

    #[test]
    fn is_cjk_japanese_hiragana() {
        assert!(is_cjk('あ'));
        assert!(is_cjk('の'));
    }

    #[test]
    fn is_cjk_japanese_katakana() {
        assert!(is_cjk('ア'));
        assert!(is_cjk('ン'));
    }

    #[test]
    fn is_cjk_korean() {
        assert!(is_cjk('한'));
        assert!(is_cjk('글'));
    }

    #[test]
    fn is_cjk_ascii_false() {
        assert!(!is_cjk('a'));
        assert!(!is_cjk('Z'));
        assert!(!is_cjk('5'));
        assert!(!is_cjk(' '));
    }

    #[test]
    fn is_cjk_latin_extended_false() {
        assert!(!is_cjk('é'));
        assert!(!is_cjk('ñ'));
    }

    // ── is_intra_word_separator ─────────────────────────────────────────

    #[test]
    fn separator_hyphen() {
        assert!(is_intra_word_separator('-'));
    }

    #[test]
    fn separator_dot() {
        assert!(is_intra_word_separator('.'));
    }

    #[test]
    fn separator_underscore() {
        assert!(is_intra_word_separator('_'));
    }

    #[test]
    fn separator_not_alphanumeric() {
        assert!(!is_intra_word_separator('a'));
        assert!(!is_intra_word_separator('5'));
    }

    #[test]
    fn separator_not_whitespace() {
        assert!(!is_intra_word_separator(' '));
        assert!(!is_intra_word_separator('\t'));
    }

    #[test]
    fn separator_not_cjk() {
        assert!(!is_intra_word_separator('中'));
    }

    #[test]
    fn separator_not_null() {
        assert!(!is_intra_word_separator('\0'));
    }

    #[test]
    fn concat_token_requires_minimum_parts_and_length() {
        assert!(should_emit_concat_token(2, "ecommerce"));
        assert!(!should_emit_concat_token(1, "ecommerce"));
        assert!(!should_emit_concat_token(2, "ab"));
    }

    // ── tokenizer: basic Latin ──────────────────────────────────────────

    #[test]
    fn tokenize_simple_english() {
        let tokens = collect_tokens("hello world");
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn tokenize_empty() {
        let tokens = collect_tokens("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_single_word() {
        let tokens = collect_tokens("flapjack");
        assert_eq!(tokens, vec!["flapjack"]);
    }

    // ── tokenizer: CJK ─────────────────────────────────────────────────

    #[test]
    fn tokenize_chinese_chars_individually() {
        let tokens = collect_tokens("中国人");
        assert_eq!(tokens, vec!["中", "国", "人"]);
    }

    #[test]
    fn tokenize_japanese_hiragana() {
        let tokens = collect_tokens("おはよう");
        assert_eq!(tokens, vec!["お", "は", "よ", "う"]);
    }

    #[test]
    fn tokenize_mixed_cjk_and_latin() {
        let tokens = collect_tokens("hello中国world");
        assert_eq!(tokens, vec!["hello", "中", "国", "world"]);
    }

    // ── tokenizer: intra-word separators (concat tokens) ────────────────

    #[test]
    fn tokenize_hyphenated_produces_parts_and_concat() {
        let tokens = collect_tokens("e-commerce");
        assert!(tokens.contains(&"e".to_string()));
        assert!(tokens.contains(&"commerce".to_string()));
        assert!(tokens.contains(&"ecommerce".to_string()));
    }

    #[test]
    fn tokenize_short_concat_skipped() {
        // "a-b" → parts "a" and "b", but concat "ab" is only 2 chars < 3, so no concat token
        let tokens = collect_tokens("a-b");
        assert!(tokens.contains(&"a".to_string()));
        assert!(tokens.contains(&"b".to_string()));
        assert!(!tokens.contains(&"ab".to_string()));
    }

    #[test]
    fn tokenize_dotted_word() {
        let tokens = collect_tokens("Dr.Smith");
        assert!(tokens.contains(&"dr".to_string()));
        assert!(tokens.contains(&"smith".to_string()));
        assert!(tokens.contains(&"drsmith".to_string()));
    }

    #[test]
    fn tokenize_normalizes_diacritics_by_default() {
        let tokens = collect_tokens("København");
        assert_eq!(tokens, vec!["kobenhavn"]);
    }

    #[test]
    fn tokenize_keeps_selected_diacritics() {
        let mut tokenizer = CjkAwareTokenizer::new().with_keep_diacritics_on_characters("ø");
        let mut stream = tokenizer.token_stream("København");
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }
        assert_eq!(tokens, vec!["københavn"]);
    }

    #[test]
    fn tokenize_indexed_separator_emits_separator_token() {
        let tokens = collect_tokens_with_indexed_separators("c++ is #1", &['+', '#']);
        assert_eq!(tokens, vec!["c", "+", "+", "is", "#", "1"]);
    }

    // ── tokenizer: positions and offsets ─────────────────────────────────

    #[test]
    fn token_positions_increment() {
        let mut tokenizer = CjkAwareTokenizer::new();
        let mut stream = tokenizer.token_stream("hello world");
        let mut positions = Vec::new();
        while stream.advance() {
            positions.push(stream.token().position);
        }
        for i in 1..positions.len() {
            assert!(positions[i] > positions[i - 1]);
        }
    }

    #[test]
    fn token_offsets_within_text() {
        let text = "hello 中国";
        let mut tokenizer = CjkAwareTokenizer::new();
        let mut stream = tokenizer.token_stream(text);
        while stream.advance() {
            let t = stream.token();
            assert!(t.offset_from <= t.offset_to);
            assert!(t.offset_to <= text.len());
        }
    }

    // ── tokenizer: whitespace edge cases ────────────────────────────────

    #[test]
    fn tokenize_multiple_spaces() {
        let tokens = collect_tokens("hello   world");
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn tokenize_only_whitespace() {
        let tokens = collect_tokens("   ");
        assert!(tokens.is_empty());
    }

    #[test]
    fn tokenize_mixed_whitespace() {
        let tokens = collect_tokens("hello\tworld\nnew");
        assert_eq!(tokens, vec!["hello", "world", "new"]);
    }

    // ── latin_only mode: CJK chars grouped into words, not split ────────

    #[test]
    fn latin_only_chinese_grouped_as_word() {
        // When cjk_splitting is disabled, CJK chars should be grouped into word tokens
        let tokens = collect_tokens_latin_only("中国人");
        assert_eq!(tokens, vec!["中国人"]);
    }

    #[test]
    fn latin_only_japanese_grouped_as_word() {
        let tokens = collect_tokens_latin_only("おはよう");
        assert_eq!(tokens, vec!["おはよう"]);
    }

    #[test]
    fn latin_only_mixed_cjk_latin_grouped() {
        // Mixed CJK+Latin without whitespace should be one token
        let tokens = collect_tokens_latin_only("hello中国world");
        assert_eq!(tokens, vec!["hello中国world"]);
    }

    #[test]
    fn latin_only_latin_unchanged() {
        // Latin text should work identically in latin_only mode
        let tokens = collect_tokens_latin_only("hello world");
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn latin_only_hyphenated_still_works() {
        // Concat token behavior should be preserved in latin_only mode
        let tokens = collect_tokens_latin_only("e-commerce");
        assert!(tokens.contains(&"e".to_string()));
        assert!(tokens.contains(&"commerce".to_string()));
        assert!(tokens.contains(&"ecommerce".to_string()));
    }

    #[test]
    fn latin_only_cjk_with_spaces_splits_on_space() {
        // CJK text with spaces should split on space boundaries
        let tokens = collect_tokens_latin_only("東京 タワー");
        assert_eq!(tokens, vec!["東京", "タワー"]);
    }

    #[test]
    fn default_is_cjk_aware() {
        // Default constructor should enable CJK splitting
        let mut tokenizer = CjkAwareTokenizer::default();
        let mut stream = tokenizer.token_stream("中国");
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }
        assert_eq!(tokens, vec!["中", "国"]);
    }
}
