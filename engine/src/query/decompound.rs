use std::collections::HashSet;

/// Languages that support decompounding (matches `LanguageCode::supports_decompound`).
pub const DECOMPOUND_LANGUAGES: &[&str] = &["de", "nl"];

/// Returns true if the given language code supports decompounding.
pub fn supports_decompound(lang: &str) -> bool {
    DECOMPOUND_LANGUAGES.contains(&lang.to_lowercase().as_str())
}

/// Trait for compound word splitting.
pub trait Decompounder: Send + Sync {
    /// Attempt to split a word into its compound parts.
    ///
    /// Returns `Some(parts)` if the word is a compound (parts.len() >= 2),
    /// or `None` if it's not a compound or can't be split.
    fn decompound(&self, word: &str) -> Option<Vec<String>>;
}

/// German Fugenlaute (linking elements) that can appear between compound parts.
/// Ordered longest-first so we try stripping longer suffixes before shorter ones.
const GERMAN_FUGEN: &[&str] = &["ens", "er", "es", "en", "ns", "e", "n", "s"];
/// Dutch linking elements used in compounds.
const DUTCH_FUGEN: &[&str] = &["en", "s", "e"];

/// Minimum length for a compound component (in chars).
const MIN_COMPONENT_LEN: usize = 3;

/// German decompounding using a dictionary-backed greedy longest-match approach.
///
/// The dictionary contains common German root words. Given a compound word, the
/// decompounder tries all binary split points and checks whether both halves
/// (after removing potential linking elements from the left part) are in the
/// dictionary. Prefers splits where the left component is longest.
pub struct GermanDecompounder {
    words: HashSet<String>,
    fugen: &'static [&'static str],
}

impl GermanDecompounder {
    /// Create a new decompounder from a newline-delimited word list.
    pub fn from_word_list(data: &str) -> Self {
        Self::from_word_list_with_fugen(data, GERMAN_FUGEN)
    }

    /// Create a new decompounder from a newline-delimited word list and
    /// language-specific linking elements.
    pub fn from_word_list_with_fugen(data: &str, fugen: &'static [&'static str]) -> Self {
        let words: HashSet<String> = data
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(|line| line.to_lowercase())
            .collect();
        Self { words, fugen }
    }

    /// Check if a word (lowercase) is in the dictionary.
    fn is_known(&self, word: &str) -> bool {
        self.words.contains(word)
    }

    /// Check if `left` is a valid left component of a compound word.
    /// Tries the word as-is first, then strips each Fugenlaut and checks.
    /// Returns the dictionary form if found.
    fn valid_left_component(&self, left: &str) -> bool {
        if self.is_known(left) {
            return true;
        }
        for fugen in self.fugen {
            if let Some(stem) = left.strip_suffix(fugen) {
                if stem.len() >= MIN_COMPONENT_LEN && self.is_known(stem) {
                    return true;
                }
            }
        }
        false
    }
}

impl Decompounder for GermanDecompounder {
    /// TODO: Document GermanDecompounder.decompound.
    fn decompound(&self, word: &str) -> Option<Vec<String>> {
        let lower = word.to_lowercase();
        let chars: Vec<char> = lower.chars().collect();
        let len = chars.len();

        if len < MIN_COMPONENT_LEN * 2 {
            return None;
        }

        // Try splits from longest-left to shortest-left (greedy longest match)
        for split_pos in (MIN_COMPONENT_LEN..=len.saturating_sub(MIN_COMPONENT_LEN)).rev() {
            let left: String = chars[..split_pos].iter().collect();
            let right: String = chars[split_pos..].iter().collect();

            if self.valid_left_component(&left) && self.is_known(&right) {
                return Some(vec![left, right]);
            }
        }

        None
    }
}

/// Global German decompounder instance, lazily initialized.
static GERMAN_DECOMPOUNDER: std::sync::OnceLock<GermanDecompounder> = std::sync::OnceLock::new();
static DUTCH_DECOMPOUNDER: std::sync::OnceLock<GermanDecompounder> = std::sync::OnceLock::new();

/// Embedded German word list for decompounding.
const GERMAN_WORD_LIST: &str = include_str!("../../package/lang/decompound/de_words_de.txt");
/// Embedded Dutch word list for decompounding.
const DUTCH_WORD_LIST: &str = include_str!("../../package/lang/decompound/nl_words_nl.txt");

/// Get the global German decompounder instance.
pub fn german_decompounder() -> &'static GermanDecompounder {
    GERMAN_DECOMPOUNDER.get_or_init(|| GermanDecompounder::from_word_list(GERMAN_WORD_LIST))
}

/// Get the global Dutch decompounder instance.
pub fn dutch_decompounder() -> &'static GermanDecompounder {
    DUTCH_DECOMPOUNDER
        .get_or_init(|| GermanDecompounder::from_word_list_with_fugen(DUTCH_WORD_LIST, DUTCH_FUGEN))
}

/// Attempt to decompound a word for the given language.
/// Returns `None` if the language doesn't support decompounding or the word
/// isn't a compound.
pub fn decompound_for_lang(word: &str, lang: &str) -> Option<Vec<String>> {
    match lang.to_lowercase().as_str() {
        "de" => german_decompounder().decompound(word),
        "nl" => dutch_decompounder().decompound(word),
        // Runtime decompound for fi/da/sv/no remains unimplemented due to
        // insufficient open-license dictionary quality.
        _ => None,
    }
}

/// Decompound a word across multiple languages. Returns the first successful
/// split, or None if no language can split it.
pub fn decompound_multi(word: &str, languages: &[String]) -> Option<Vec<String>> {
    for lang in languages {
        if let Some(parts) = decompound_for_lang(word, lang) {
            return Some(parts);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test German decompounder with a minimal hardcoded word list.
    ///
    /// Returns a GermanDecompounder instance populated with a small set of test words (hund, hütte, kinder, garten, etc.) for use in decomposition unit tests. Includes common German roots and compounds to test various splitting scenarios including Fugenlaute handling.
    fn test_decompounder() -> GermanDecompounder {
        // Small dictionary for testing - just the words we need
        GermanDecompounder::from_word_list(
            "# Test German word list\n\
             hund\n\
             hütte\n\
             kinder\n\
             garten\n\
             kind\n\
             haus\n\
             tür\n\
             auto\n\
             bahn\n\
             schule\n\
             bus\n\
             blume\n\
             topf\n\
             fuß\n\
             ball\n\
             fußball\n\
             tag\n\
             geburt\n\
             licht\n\
             arbeit\n\
             platz\n\
             geber\n\
             frau\n\
             arzt\n\
             zeit\n\
             ung\n\
             wort\n\
             buch\n\
             laden\n\
             schrank\n\
             küche\n\
             kühl\n\
             hand\n\
             tasche\n\
             schlüssel\n\
             wasser\n\
             kraft\n\
             werk\n\
             berg\n\
             straße\n\
             brot\n\
             butter\n\
             milch\n\
             kaffee\n\
             tasse\n",
        )
    }

    #[test]
    fn test_hundehütte_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Hundehütte");
        assert!(result.is_some(), "Hundehütte should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "hunde");
        assert_eq!(parts[1], "hütte");
    }

    #[test]
    fn test_kindergarten_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Kindergarten");
        assert!(result.is_some(), "Kindergarten should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "kinder");
        assert_eq!(parts[1], "garten");
    }

    #[test]
    fn test_haustür_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Haustür");
        assert!(result.is_some(), "Haustür should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "haus");
        assert_eq!(parts[1], "tür");
    }

    #[test]
    fn test_autobahn_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Autobahn");
        assert!(result.is_some(), "Autobahn should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "auto");
        assert_eq!(parts[1], "bahn");
    }

    /// Verify that Schulbus does not split due to -e elision limitation.
    ///
    /// Documents a known limitation: Schulbus (Schule + Bus) fails to decompose because the algorithm receives "schul" as the left component, which is not in the dictionary. Proper splitting would require recognizing that "Schule" loses its final -e when used as a compound component (Auslaut elision), not just Fugenlaut stripping. This can be addressed by adding combining form aliases to the word list.
    #[test]
    fn test_schulbus_not_supported_yet() {
        let dc = test_decompounder();
        // "Schulbus" = Schule + Bus, with -e Fugenlaut on "Schule" stripped
        // But "Schul" is left part. schul + bus. "schul" isn't in dict,
        // but schule is and "schule" - strip nothing = schule? No, left="schul",
        // strip nothing doesn't work. Actually the split is at pos 5: "schul" + "bus"
        // "schul" → not in dict. Strip nothing. Try fugen: schul-e? No, we strip
        // fugen from left. Actually "schul" is not in dict. Let's try split at pos 6:
        // "schulb" + "us" — too short. So this would need "schul" in dict OR special handling.
        // Actually Schulbus: the left part is "Schul" which is the combining form of "Schule".
        // In German compounds, the Fugen concept works the OTHER direction here:
        // the root is "Schule" and the -e is dropped, making "Schul" + "Bus".
        // This is actually an Auslaut (final sound) deletion, not a Fugenlaut addition.
        //
        // For now, our algorithm won't catch this since "schul" isn't in the dict.
        // This is a known limitation. We could add "schul" as an alias in the word list.
        let result = dc.decompound("Schulbus");
        assert!(
            result.is_none(),
            "Schulbus should not split until -e elision is implemented"
        );
    }

    #[test]
    fn test_blumentopf_splits() {
        let dc = test_decompounder();
        // Blumentopf = Blume + n + Topf
        // Split at pos 6: "blumen" + "topf"
        // "blumen" → strip -n → "blume" → in dict ✓
        // "topf" → in dict ✓
        let result = dc.decompound("Blumentopf");
        assert!(result.is_some(), "Blumentopf should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "blumen");
        assert_eq!(parts[1], "topf");
    }

    #[test]
    fn test_geburtstag_splits() {
        let dc = test_decompounder();
        // Geburtstag = Geburt + s + Tag
        // Split at pos 7: "geburts" + "tag"
        // "geburts" → strip -s → "geburt" → in dict ✓
        // "tag" → in dict ✓
        let result = dc.decompound("Geburtstag");
        assert!(result.is_some(), "Geburtstag should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "geburts");
        assert_eq!(parts[1], "tag");
    }

    #[test]
    fn test_fußball_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Fußball");
        assert!(result.is_some(), "Fußball should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "fuß");
        assert_eq!(parts[1], "ball");
    }

    #[test]
    fn test_wasserkraft_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Wasserkraft");
        assert!(result.is_some(), "Wasserkraft should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "wasser");
        assert_eq!(parts[1], "kraft");
    }

    #[test]
    fn test_non_compound_passes_through() {
        let dc = test_decompounder();
        assert!(dc.decompound("Hund").is_none());
        assert!(dc.decompound("Ball").is_none());
        assert!(dc.decompound("Tag").is_none());
    }

    #[test]
    fn test_short_words_pass_through() {
        let dc = test_decompounder();
        assert!(dc.decompound("ab").is_none());
        assert!(dc.decompound("zu").is_none());
        assert!(dc.decompound("").is_none());
    }

    #[test]
    fn test_unknown_word_passes_through() {
        let dc = test_decompounder();
        assert!(dc.decompound("xyzabc").is_none());
        assert!(dc.decompound("qqqqrrrr").is_none());
    }

    #[test]
    fn test_case_insensitive() {
        let dc = test_decompounder();
        let r1 = dc.decompound("hundehütte");
        let r2 = dc.decompound("HUNDEHÜTTE");
        let r3 = dc.decompound("Hundehütte");
        assert!(r1.is_some());
        assert!(r2.is_some());
        assert!(r3.is_some());
        // All should produce same lowercase result
        assert_eq!(r1.unwrap(), r2.unwrap());
    }

    #[test]
    fn test_supports_decompound_fn() {
        assert!(supports_decompound("de"));
        assert!(supports_decompound("nl"));
        assert!(!supports_decompound("fi"));
        assert!(!supports_decompound("da"));
        assert!(!supports_decompound("sv"));
        assert!(!supports_decompound("no"));
        assert!(!supports_decompound("en"));
        assert!(!supports_decompound("fr"));
        assert!(!supports_decompound(""));
    }

    #[test]
    fn test_decompound_for_lang_unsupported() {
        assert!(decompound_for_lang("something", "en").is_none());
        assert!(decompound_for_lang("something", "fr").is_none());
        assert!(decompound_for_lang("something", "xx").is_none());
    }

    #[test]
    fn test_decompound_multi_first_match_wins() {
        let langs = vec!["en".to_string(), "de".to_string()];
        // "en" has no decompounder, "de" should match for compounds
        let result = decompound_multi("Hundehütte", &langs);
        assert_eq!(result, Some(vec!["hunde".to_string(), "hütte".to_string()]));
    }

    #[test]
    fn test_decompound_multi_no_decompound_langs() {
        let langs = vec!["en".to_string(), "fr".to_string()];
        assert!(decompound_multi("Hundehütte", &langs).is_none());
    }

    #[test]
    fn test_word_list_ignores_comments_and_blanks() {
        let dc =
            GermanDecompounder::from_word_list("# comment\n\nhund\n  hütte  \n# another comment\n");
        assert!(dc.is_known("hund"));
        assert!(dc.is_known("hütte"));
        assert!(!dc.is_known("# comment"));
        assert!(!dc.is_known(""));
    }

    #[test]
    fn test_global_german_decompounder_loads() {
        // Verify the embedded word list loads without panic
        let dc = german_decompounder();
        // Should have a reasonable number of words
        assert!(
            dc.words.len() > 100,
            "German word list should have >100 words, got {}",
            dc.words.len()
        );
    }

    #[test]
    fn test_global_decompounder_hundehütte() {
        // Test the global decompounder with embedded dictionary
        let result = decompound_for_lang("Hundehütte", "de");
        assert_eq!(
            result,
            Some(vec!["hunde".to_string(), "hütte".to_string()]),
            "Global German decompounder should split Hundehütte deterministically"
        );
    }

    #[test]
    fn test_dutch_voetbal_splits() {
        let result = decompound_for_lang("voetbal", "nl");
        assert_eq!(
            result,
            Some(vec!["voet".to_string(), "bal".to_string()]),
            "Dutch decompounder should split voetbal -> voet + bal"
        );
    }

    #[test]
    fn test_dutch_schoolboek_splits() {
        let result = decompound_for_lang("schoolboek", "nl");
        assert_eq!(
            result,
            Some(vec!["school".to_string(), "boek".to_string()]),
            "Dutch decompounder should split schoolboek -> school + boek"
        );
    }

    #[test]
    fn test_dutch_non_compound_passthrough() {
        assert!(
            decompound_for_lang("fiets", "nl").is_none(),
            "Dutch non-compound words should pass through"
        );
    }

    #[test]
    fn test_handtasche_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Handtasche");
        assert!(result.is_some(), "Handtasche should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts[0], "hand");
        assert_eq!(parts[1], "tasche");
    }

    #[test]
    fn test_butterbrot_splits() {
        let dc = test_decompounder();
        let result = dc.decompound("Butterbrot");
        assert!(result.is_some(), "Butterbrot should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts[0], "butter");
        assert_eq!(parts[1], "brot");
    }

    #[test]
    fn test_kaffeetasse_splits() {
        let dc = test_decompounder();
        // Kaffeetasse = Kaffee + Tasse (no Fugenlaut, "kaffee" is in dict directly)
        let result = dc.decompound("Kaffeetasse");
        assert!(result.is_some(), "Kaffeetasse should be decompounded");
        let parts = result.unwrap();
        assert_eq!(parts[0], "kaffee");
        assert_eq!(parts[1], "tasse");
    }
}
