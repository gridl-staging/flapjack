//! Text normalization utilities for search, including diacritic removal with exceptions, character folding, and camelCase word splitting.
use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};

/// Normalize text for search by lowercasing, removing diacritics, and applying character decomposition. Applies custom normalization mappings with highest priority, then uses NFKD decomposition to handle most diacritics (é → e), and falls back to supplementary folding for characters that don't decompose (ø → o, æ → ae). Characters in `keep_diacritics_on_characters` preserve their diacritical marks.
///
/// # Arguments
///
/// * `text` - The input text to normalize
/// * `keep_diacritics_on_characters` - Characters that should retain their diacritics during normalization
/// * `custom_normalization` - Custom character-to-string mappings applied before built-in folding
///
/// # Returns
///
/// Normalized string with lowercase characters, diacritics removed (except exceptions), and custom mappings applied.
pub(crate) fn normalize_for_search(
    text: &str,
    keep_diacritics_on_characters: &str,
    custom_normalization: &[(char, String)],
) -> String {
    let keep: Vec<char> = keep_diacritics_on_characters
        .chars()
        .flat_map(|c| c.to_lowercase())
        .collect();

    let mut normalized = String::with_capacity(text.len());

    for original in text.chars() {
        for lowered in original.to_lowercase() {
            if keep.contains(&lowered) {
                normalized.push(lowered);
                continue;
            }

            // Custom normalization mappings take priority over all built-in folding.
            if let Some((_, replacement)) = custom_normalization.iter().find(|(c, _)| *c == lowered)
            {
                normalized.push_str(replacement);
                continue;
            }

            // NFKD decomposition + combining mark removal handles most diacritics
            // (e.g. é → e, ñ → n). For characters that don't decompose via NFKD
            // (e.g. ø, æ, đ), fall back to a supplementary mapping.
            let mut all_ascii = true;
            let mut decomposed_buf = String::new();
            for decomposed in lowered.nfkd() {
                if is_combining_mark(decomposed) {
                    continue;
                }
                if !decomposed.is_ascii() {
                    all_ascii = false;
                }
                decomposed_buf.push(decomposed);
            }

            if all_ascii {
                normalized.push_str(&decomposed_buf);
            } else {
                for c in decomposed_buf.chars() {
                    if c.is_ascii() {
                        normalized.push(c);
                    } else {
                        let folded = fold_non_decomposable(c);
                        if folded.is_empty() {
                            normalized.push(c);
                        } else {
                            normalized.push_str(folded);
                        }
                    }
                }
            }
        }
    }

    normalized
}

/// Check whether `path` matches or is nested under any attribute in `camel_case_attributes`.
pub(crate) fn is_camel_case_attr_path(path: &str, camel_case_attributes: &[String]) -> bool {
    camel_case_attributes.iter().any(|attr| {
        path == attr
            || path
                .strip_prefix(attr.as_str())
                .is_some_and(|suffix| suffix.starts_with('.'))
    })
}

/// Maps Unicode characters that NFKD normalization doesn't decompose to their
/// ASCII equivalents. Covers the most common European characters.
fn fold_non_decomposable(c: char) -> &'static str {
    match c {
        'æ' => "ae",
        'ø' => "o",
        'đ' | 'ð' => "d",
        'ł' => "l",
        'ħ' => "h",
        'ŋ' => "ng",
        'ß' => "ss",
        'þ' => "th",
        'ŧ' => "t",
        'ĸ' => "k",
        'œ' => "oe",
        _ => "",
    }
}

/// Insert spaces between words in camelCase identifiers, preserving the original characters. Splits on lowercase-to-uppercase transitions, digit-to-letter transitions, and consecutive uppercase letters followed by lowercase (JSONParser → JSON Parser).
///
/// # Arguments
///
/// * `text` - The input text with camelCase identifiers
///
/// # Returns
///
/// String with spaces inserted at camelCase word boundaries.
pub(crate) fn split_camel_case_words(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut prev = None::<char>;

    while let Some(curr) = chars.next() {
        let next = chars.peek().copied();
        if let Some(previous) = prev {
            if should_split_on_camel_transition(previous, curr, next) && !out.is_empty() {
                out.push(' ');
            }
        }
        out.push(curr);
        prev = Some(curr);
    }

    out
}

/// Determine if a space should be inserted between two characters during camelCase splitting. Returns `true` for transitions like lowercase→uppercase, digit→letter, letter→digit, and consecutive uppercase followed by lowercase.
///
/// # Arguments
///
/// * `previous` - The character before the potential split point
/// * `current` - The character at the split point
/// * `next` - The character after the current one, if any
///
/// # Returns
///
/// `true` if a split should occur at this transition, `false` otherwise.
fn should_split_on_camel_transition(previous: char, current: char, next: Option<char>) -> bool {
    if !(previous.is_ascii_alphanumeric() && current.is_ascii_alphanumeric()) {
        return false;
    }

    if (previous.is_ascii_lowercase() && current.is_ascii_uppercase())
        || (previous.is_ascii_digit() && current.is_ascii_alphabetic())
        || (previous.is_ascii_alphabetic() && current.is_ascii_digit())
    {
        return true;
    }

    previous.is_ascii_uppercase()
        && current.is_ascii_uppercase()
        && next.is_some_and(|n| n.is_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::normalize_for_search;
    use super::split_camel_case_words;

    #[test]
    fn normalizes_diacritics_by_default() {
        assert_eq!(normalize_for_search("København", "", &[]), "kobenhavn");
    }

    #[test]
    fn keeps_selected_characters() {
        assert_eq!(normalize_for_search("København", "ø", &[]), "københavn");
    }

    #[test]
    fn preserves_non_latin_characters_without_fold_mapping() {
        assert_eq!(normalize_for_search("Hello中国", "", &[]), "hello中国");
    }

    #[test]
    fn split_camel_case_into_word_parts() {
        assert_eq!(split_camel_case_words("macBookPro"), "mac Book Pro");
    }

    #[test]
    fn split_camel_case_handles_mixed_uppercase_runs() {
        assert_eq!(split_camel_case_words("JSONParser"), "JSON Parser");
    }

    #[test]
    fn split_camel_case_keeps_plain_text() {
        assert_eq!(split_camel_case_words("simple"), "simple");
    }

    #[test]
    fn custom_normalization_maps_char() {
        // Custom mapping: ğ → g
        let custom = vec![('ğ', "g".to_string())];
        assert_eq!(normalize_for_search("Erdoğan", "", &custom), "erdogan");
    }

    #[test]
    fn custom_normalization_overrides_builtin_fold() {
        // Built-in maps ß → ss, but custom overrides to sz
        let custom = vec![('ß', "sz".to_string())];
        assert_eq!(normalize_for_search("Straße", "", &custom), "strasze");
    }

    #[test]
    fn custom_normalization_empty_is_noop() {
        // Empty custom normalization preserves existing behavior
        let custom: Vec<(char, String)> = vec![];
        assert_eq!(normalize_for_search("København", "", &custom), "kobenhavn");
    }
}
