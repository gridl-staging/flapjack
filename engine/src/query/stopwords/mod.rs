use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

const SUPPORTED_STOPWORD_LANGS: &[&str] = &[
    "ar", "bg", "ca", "cs", "da", "de", "el", "en", "es", "fi", "fr", "ga", "hi", "hu", "id", "it",
    "ja", "ko", "lt", "nl", "no", "pl", "pt", "ro", "ru", "sv", "th", "tr", "uk", "zh",
];

const STOPWORDS_AR: &str = include_str!("../../../package/lang/stopwords/ar.txt");
const STOPWORDS_BG: &str = include_str!("../../../package/lang/stopwords/bg.txt");
const STOPWORDS_CA: &str = include_str!("../../../package/lang/stopwords/ca.txt");
const STOPWORDS_CS: &str = include_str!("../../../package/lang/stopwords/cs.txt");
const STOPWORDS_DA: &str = include_str!("../../../package/lang/stopwords/da.txt");
const STOPWORDS_DE: &str = include_str!("../../../package/lang/stopwords/de.txt");
const STOPWORDS_EL: &str = include_str!("../../../package/lang/stopwords/el.txt");
const STOPWORDS_EN: &str = include_str!("../../../package/lang/stopwords/en.txt");
const STOPWORDS_ES: &str = include_str!("../../../package/lang/stopwords/es.txt");
const STOPWORDS_FI: &str = include_str!("../../../package/lang/stopwords/fi.txt");
const STOPWORDS_FR: &str = include_str!("../../../package/lang/stopwords/fr.txt");
const STOPWORDS_GA: &str = include_str!("../../../package/lang/stopwords/ga.txt");
const STOPWORDS_HI: &str = include_str!("../../../package/lang/stopwords/hi.txt");
const STOPWORDS_HU: &str = include_str!("../../../package/lang/stopwords/hu.txt");
const STOPWORDS_ID: &str = include_str!("../../../package/lang/stopwords/id.txt");
const STOPWORDS_IT: &str = include_str!("../../../package/lang/stopwords/it.txt");
const STOPWORDS_JA: &str = include_str!("../../../package/lang/stopwords/ja.txt");
const STOPWORDS_KO: &str = include_str!("../../../package/lang/stopwords/ko.txt");
const STOPWORDS_LT: &str = include_str!("../../../package/lang/stopwords/lt.txt");
const STOPWORDS_NL: &str = include_str!("../../../package/lang/stopwords/nl.txt");
const STOPWORDS_NO: &str = include_str!("../../../package/lang/stopwords/no.txt");
const STOPWORDS_PL: &str = include_str!("../../../package/lang/stopwords/pl.txt");
const STOPWORDS_PT: &str = include_str!("../../../package/lang/stopwords/pt.txt");
const STOPWORDS_RO: &str = include_str!("../../../package/lang/stopwords/ro.txt");
const STOPWORDS_RU: &str = include_str!("../../../package/lang/stopwords/ru.txt");
const STOPWORDS_SV: &str = include_str!("../../../package/lang/stopwords/sv.txt");
const STOPWORDS_TH: &str = include_str!("../../../package/lang/stopwords/th.txt");
const STOPWORDS_TR: &str = include_str!("../../../package/lang/stopwords/tr.txt");
const STOPWORDS_UK: &str = include_str!("../../../package/lang/stopwords/uk.txt");
const STOPWORDS_ZH: &str = include_str!("../../../package/lang/stopwords/zh.txt");

static STOPWORD_SET_CACHE: OnceLock<HashMap<&'static str, HashSet<&'static str>>> = OnceLock::new();

fn canonical_stopword_lang(lang: &str) -> String {
    match lang.to_ascii_lowercase().as_str() {
        "pt-br" | "ptbr" => "pt".to_string(),
        other => other.to_string(),
    }
}

/// Retrieve the embedded stopword data string for a language code.
///
/// # Arguments
///
/// * `lang` - Two-letter language code (e.g., "en", "fr", "de")
///
/// # Returns
///
/// A static reference to the stopword list if the language is supported, otherwise None.
fn raw_stopword_data_for_lang(lang: &str) -> Option<&'static str> {
    match lang {
        "ar" => Some(STOPWORDS_AR),
        "bg" => Some(STOPWORDS_BG),
        "ca" => Some(STOPWORDS_CA),
        "cs" => Some(STOPWORDS_CS),
        "da" => Some(STOPWORDS_DA),
        "de" => Some(STOPWORDS_DE),
        "el" => Some(STOPWORDS_EL),
        "en" => Some(STOPWORDS_EN),
        "es" => Some(STOPWORDS_ES),
        "fi" => Some(STOPWORDS_FI),
        "fr" => Some(STOPWORDS_FR),
        "ga" => Some(STOPWORDS_GA),
        "hi" => Some(STOPWORDS_HI),
        "hu" => Some(STOPWORDS_HU),
        "id" => Some(STOPWORDS_ID),
        "it" => Some(STOPWORDS_IT),
        "ja" => Some(STOPWORDS_JA),
        "ko" => Some(STOPWORDS_KO),
        "lt" => Some(STOPWORDS_LT),
        "nl" => Some(STOPWORDS_NL),
        "no" => Some(STOPWORDS_NO),
        "pl" => Some(STOPWORDS_PL),
        "pt" => Some(STOPWORDS_PT),
        "ro" => Some(STOPWORDS_RO),
        "ru" => Some(STOPWORDS_RU),
        "sv" => Some(STOPWORDS_SV),
        "th" => Some(STOPWORDS_TH),
        "tr" => Some(STOPWORDS_TR),
        "uk" => Some(STOPWORDS_UK),
        "zh" => Some(STOPWORDS_ZH),
        _ => None,
    }
}

fn parse_stopword_lines(data: &'static str) -> Vec<&'static str> {
    data.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

#[cfg(test)]
fn stopword_list_for_lang(lang: &str) -> Option<Vec<&'static str>> {
    let canonical = canonical_stopword_lang(lang);
    raw_stopword_data_for_lang(canonical.as_str()).map(parse_stopword_lines)
}

fn build_stopword_set_cache() -> HashMap<&'static str, HashSet<&'static str>> {
    SUPPORTED_STOPWORD_LANGS
        .iter()
        .map(|lang| {
            let set: HashSet<&'static str> = parse_stopword_lines(
                raw_stopword_data_for_lang(lang).expect("supported language must have raw data"),
            )
            .into_iter()
            .collect();
            (*lang, set)
        })
        .collect()
}

/// Get the stopword set for a given language code.
/// Returns None for languages without stopword data.
pub fn stopwords_for_lang(lang: &str) -> Option<HashSet<&'static str>> {
    let canonical = canonical_stopword_lang(lang);
    let cache = STOPWORD_SET_CACHE.get_or_init(build_stopword_set_cache);
    cache.get(canonical.as_str()).cloned()
}

pub fn english_stop_words() -> HashSet<&'static str> {
    stopwords_for_lang("en").unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Default)]
pub enum RemoveStopWordsValue {
    #[default]
    Disabled,
    All,
    Languages(Vec<String>),
}

impl RemoveStopWordsValue {
    pub fn is_enabled_for(&self, lang: &str) -> bool {
        let canonical_lang = canonical_stopword_lang(lang);
        match self {
            RemoveStopWordsValue::Disabled => false,
            RemoveStopWordsValue::All => true,
            RemoveStopWordsValue::Languages(langs) => langs
                .iter()
                .any(|l| canonical_stopword_lang(l) == canonical_lang),
        }
    }
}

impl serde::Serialize for RemoveStopWordsValue {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            RemoveStopWordsValue::Disabled => serializer.serialize_bool(false),
            RemoveStopWordsValue::All => serializer.serialize_bool(true),
            RemoveStopWordsValue::Languages(langs) => langs.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for RemoveStopWordsValue {
    /// Deserialize from either a boolean (enable/disable with defaults), a list of
    /// language codes, or a map with explicit language lists.
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de;

        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = RemoveStopWordsValue;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("bool or array of language codes")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Self::Value, E> {
                if v {
                    Ok(RemoveStopWordsValue::All)
                } else {
                    Ok(RemoveStopWordsValue::Disabled)
                }
            }

            fn visit_seq<A: de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut langs = Vec::new();
                while let Some(val) = seq.next_element::<String>()? {
                    langs.push(val);
                }
                if langs.is_empty() {
                    Ok(RemoveStopWordsValue::Disabled)
                } else {
                    Ok(RemoveStopWordsValue::Languages(langs))
                }
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// Remove stopwords from a query string based on language settings and query type.
///
/// # Arguments
///
/// * `query` - The input query string
/// * `setting` - Which languages' stopwords to remove: Disabled (none), All (use query_languages or default to English), or Languages (specific list)
/// * `query_type` - How to handle prefix tokens: "prefixAll" preserves all words, "prefixLast" preserves the last word unless there's trailing whitespace
/// * `query_languages` - Languages used when setting is All; if empty, defaults to English
///
/// # Returns
///
/// The filtered query with stopwords removed. If all words would be removed, returns the original query to prevent empty searches.
pub fn remove_stop_words_with_query_languages(
    query: &str,
    setting: &RemoveStopWordsValue,
    query_type: &str,
    query_languages: &[String],
) -> String {
    let langs: Vec<&str> = match setting {
        RemoveStopWordsValue::Disabled => return query.to_string(),
        RemoveStopWordsValue::All => {
            if query_languages.is_empty() {
                vec!["en"]
            } else {
                query_languages.iter().map(|s| s.as_str()).collect()
            }
        }
        RemoveStopWordsValue::Languages(langs) => langs.iter().map(|s| s.as_str()).collect(),
    };

    remove_stop_words_inner(query, &langs, query_type)
}

/// Filter stopword tokens from a query for specified languages, respecting prefix query semantics.
///
/// # Arguments
///
/// * `query` - The original query string
/// * `langs` - Language codes whose stopwords to filter
/// * `query_type` - Query type affecting token preservation: "prefixAll" keeps all tokens, "prefixLast" keeps the last token (unless trailing space)
///
/// # Returns
///
/// Filtered query with stopwords removed and trailing space preserved if present. Returns original query if all tokens would be removed.
fn remove_stop_words_inner(query: &str, langs: &[&str], query_type: &str) -> String {
    let mut all_stop_words = HashSet::new();
    for lang in langs {
        if let Some(sw) = stopwords_for_lang(lang) {
            all_stop_words.extend(sw);
        }
    }

    if all_stop_words.is_empty() {
        return query.to_string();
    }

    let words: Vec<&str> = query.split_whitespace().collect();
    if words.is_empty() {
        return query.to_string();
    }

    let trailing_space = query.ends_with(' ');
    let last_idx = words.len() - 1;

    let filtered: Vec<&str> = words
        .iter()
        .enumerate()
        .filter(|(i, w)| {
            let is_prefix_token = match query_type {
                "prefixAll" => true,
                "prefixLast" => *i == last_idx && !trailing_space,
                _ => false,
            };
            if is_prefix_token {
                return true;
            }
            !all_stop_words.contains(w.to_lowercase().as_str())
        })
        .map(|(_, w)| *w)
        .collect();

    if filtered.is_empty() {
        return query.to_string();
    }

    let mut result = filtered.join(" ");
    if trailing_space {
        result.push(' ');
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: calls the production function with empty query_languages (defaults to English for All).
    fn remove_sw(query: &str, setting: &RemoveStopWordsValue, query_type: &str) -> String {
        remove_stop_words_with_query_languages(query, setting, query_type, &[])
    }

    /// Helper: calls the production function with specific query_languages.
    fn remove_sw_langs(
        query: &str,
        setting: &RemoveStopWordsValue,
        query_type: &str,
        langs: &[String],
    ) -> String {
        remove_stop_words_with_query_languages(query, setting, query_type, langs)
    }

    #[test]
    fn test_disabled_noop() {
        let r = remove_sw(
            "the best search engine",
            &RemoveStopWordsValue::Disabled,
            "prefixLast",
        );
        assert_eq!(r, "the best search engine");
    }

    #[test]
    fn test_basic_removal() {
        let r = remove_sw(
            "the best search engine",
            &RemoveStopWordsValue::All,
            "prefixNone",
        );
        assert_eq!(r, "best search engine");
    }

    #[test]
    fn test_prefix_last_preserves_last_word() {
        let r = remove_sw("what is the", &RemoveStopWordsValue::All, "prefixLast");
        assert_eq!(r, "the");
    }

    #[test]
    fn test_prefix_last_trailing_space_all_stopwords_preserves_original() {
        // Trailing space means last word is NOT a prefix token, so all 3 are stopwords.
        // When all words are filtered, we preserve the original query to avoid empty search.
        let r = remove_sw("what is the ", &RemoveStopWordsValue::All, "prefixLast");
        assert_eq!(r, "what is the ");
    }

    #[test]
    fn test_prefix_all_preserves_all() {
        let r = remove_sw("the a is", &RemoveStopWordsValue::All, "prefixAll");
        assert_eq!(r, "the a is");
    }

    #[test]
    fn test_all_stop_words_preserves_original() {
        let r = remove_sw("the a an", &RemoveStopWordsValue::All, "prefixNone");
        assert_eq!(r, "the a an");
    }

    #[test]
    fn test_language_specific() {
        let r = remove_sw_langs(
            "the best engine",
            &RemoveStopWordsValue::Languages(vec!["en".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "best engine");
    }

    #[test]
    fn test_unsupported_language_noop() {
        let r = remove_sw_langs(
            "the best engine",
            &RemoveStopWordsValue::Languages(vec!["xx".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "the best engine");
    }

    #[test]
    fn test_case_insensitive() {
        let r = remove_sw(
            "The Best IS engine",
            &RemoveStopWordsValue::All,
            "prefixNone",
        );
        assert_eq!(r, "Best engine");
    }

    #[test]
    fn test_empty_query() {
        let r = remove_sw("", &RemoveStopWordsValue::All, "prefixNone");
        assert_eq!(r, "");
    }

    #[test]
    fn test_serde_bool_true() {
        let v: RemoveStopWordsValue = serde_json::from_str("true").unwrap();
        assert_eq!(v, RemoveStopWordsValue::All);
        assert_eq!(serde_json::to_string(&v).unwrap(), "true");
    }

    #[test]
    fn test_serde_bool_false() {
        let v: RemoveStopWordsValue = serde_json::from_str("false").unwrap();
        assert_eq!(v, RemoveStopWordsValue::Disabled);
        assert_eq!(serde_json::to_string(&v).unwrap(), "false");
    }

    #[test]
    fn test_serde_languages() {
        let v: RemoveStopWordsValue = serde_json::from_str(r#"["en","fr"]"#).unwrap();
        assert_eq!(
            v,
            RemoveStopWordsValue::Languages(vec!["en".to_string(), "fr".to_string()])
        );
        assert_eq!(serde_json::to_string(&v).unwrap(), r#"["en","fr"]"#);
    }

    #[test]
    fn test_is_enabled_for_is_case_insensitive_and_alias_aware() {
        let v = RemoveStopWordsValue::Languages(vec!["PT-BR".to_string(), "Fr".to_string()]);
        assert!(v.is_enabled_for("pt"));
        assert!(v.is_enabled_for("pt-br"));
        assert!(v.is_enabled_for("fr"));
        assert!(!v.is_enabled_for("en"));
    }

    #[test]
    fn test_mixed_stop_and_content_words() {
        let r = remove_sw(
            "how to build a search engine",
            &RemoveStopWordsValue::All,
            "prefixLast",
        );
        assert_eq!(r, "build search engine");
    }

    #[test]
    fn test_preserves_trailing_space() {
        let r = remove_sw("best search ", &RemoveStopWordsValue::All, "prefixLast");
        assert_eq!(r, "best search ");
    }

    // ── Multi-language stopword tests (Stage 2 C) ──

    #[test]
    fn test_french_stopwords() {
        // "le" and "de" are French stopwords; "meilleur" and "moteur" are not
        let r = remove_sw_langs(
            "le meilleur moteur de recherche",
            &RemoveStopWordsValue::Languages(vec!["fr".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "meilleur moteur recherche");
    }

    #[test]
    fn test_german_stopwords() {
        // "die" and "ist" are German stopwords; "beste" and "Suchmaschine" are not
        let r = remove_sw_langs(
            "die beste Suchmaschine ist hier",
            &RemoveStopWordsValue::Languages(vec!["de".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "beste Suchmaschine");
    }

    #[test]
    fn test_spanish_stopwords() {
        // "el" and "de" are Spanish stopwords
        let r = remove_sw_langs(
            "el mejor motor de busqueda",
            &RemoveStopWordsValue::Languages(vec!["es".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "mejor motor busqueda");
    }

    #[test]
    fn test_multi_language_stopwords() {
        // With both English and French, both "the" (en) and "le" (fr) are removed
        let r = remove_sw_langs(
            "the le search recherche",
            &RemoveStopWordsValue::Languages(vec!["en".to_string(), "fr".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "search recherche");
    }

    #[test]
    fn test_stopwords_all_uses_query_languages() {
        let r = remove_sw_langs(
            "le meilleur moteur de recherche",
            &RemoveStopWordsValue::All,
            "prefixNone",
            &["fr".to_string()],
        );
        assert_eq!(r, "meilleur moteur recherche");
    }

    #[test]
    fn test_stopwords_all_defaults_to_english() {
        let r = remove_sw_langs(
            "the best search engine",
            &RemoveStopWordsValue::All,
            "prefixNone",
            &[],
        );
        assert_eq!(r, "best search engine");
    }

    #[test]
    fn test_dutch_stopwords() {
        let r = remove_sw_langs(
            "de beste zoekmachine van het web",
            &RemoveStopWordsValue::Languages(vec!["nl".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "beste zoekmachine web");
    }

    #[test]
    fn test_italian_stopwords() {
        let r = remove_sw_langs(
            "il miglior motore di ricerca",
            &RemoveStopWordsValue::Languages(vec!["it".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "miglior motore ricerca");
    }

    #[test]
    fn test_portuguese_stopwords() {
        let r = remove_sw_langs(
            "o melhor motor de busca",
            &RemoveStopWordsValue::Languages(vec!["pt".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "melhor motor busca");
    }

    #[test]
    fn test_brazilian_portuguese_alias_stopwords() {
        let r = remove_sw_langs(
            "o melhor motor de busca",
            &RemoveStopWordsValue::Languages(vec!["pt-br".to_string()]),
            "prefixNone",
            &[],
        );
        assert_eq!(r, "melhor motor busca");
    }

    #[test]
    fn test_stopwords_all_with_pt_br_query_language() {
        let r = remove_sw_langs(
            "o melhor motor de busca",
            &RemoveStopWordsValue::All,
            "prefixNone",
            &["pt-br".to_string()],
        );
        assert_eq!(r, "melhor motor busca");
    }

    // ── Per-language data validation tests ──

    /// Assert that all 30 supported language codes return non-empty stopword sets.
    #[test]
    fn test_known_languages_return_some() {
        let langs = [
            "ar", "bg", "ca", "cs", "da", "de", "el", "en", "es", "fi", "fr", "ga", "hi", "hu",
            "id", "it", "ja", "ko", "lt", "nl", "no", "pl", "pt", "ro", "ru", "sv", "th", "tr",
            "uk", "zh",
        ];
        for lang in &langs {
            let set = stopwords_for_lang(lang);
            assert!(set.is_some(), "Expected stopwords for lang '{}'", lang);
            assert!(
                !set.unwrap().is_empty(),
                "Stopwords for '{}' should not be empty",
                lang
            );
        }
    }

    #[test]
    fn test_unknown_language_returns_none() {
        assert!(stopwords_for_lang("xx").is_none());
        assert!(stopwords_for_lang("").is_none());
        assert!(stopwords_for_lang("zz").is_none());
    }

    #[test]
    fn test_case_insensitive_lookup() {
        assert!(stopwords_for_lang("EN").is_some());
        assert!(stopwords_for_lang("Fr").is_some());
        assert!(stopwords_for_lang("DE").is_some());
        let en = stopwords_for_lang("EN").unwrap();
        assert!(
            en.contains("the"),
            "uppercase 'EN' should return English stopwords"
        );
    }

    #[test]
    fn test_french_contains_common_words() {
        let fr = stopwords_for_lang("fr").unwrap();
        assert!(fr.contains("le"));
        assert!(fr.contains("la"));
        assert!(fr.contains("les"));
        assert!(fr.contains("de"));
        assert!(fr.contains("et"));
    }

    #[test]
    fn test_german_contains_common_words() {
        let de = stopwords_for_lang("de").unwrap();
        assert!(de.contains("der"));
        assert!(de.contains("die"));
        assert!(de.contains("das"));
        assert!(de.contains("und"));
        assert!(de.contains("ist"));
    }

    #[test]
    fn test_spanish_contains_common_words() {
        let es = stopwords_for_lang("es").unwrap();
        assert!(es.contains("el"));
        assert!(es.contains("la"));
        assert!(es.contains("de"));
        assert!(es.contains("que"));
        assert!(es.contains("en"));
    }

    #[test]
    fn test_no_duplicates() {
        for lang in SUPPORTED_STOPWORD_LANGS {
            let list = stopword_list_for_lang(lang).expect("expected stopword list");
            let set: HashSet<&str> = list.iter().copied().collect();
            assert_eq!(
                set.len(),
                list.len(),
                "Duplicate stopwords found in '{}': set has {} but list has {}",
                *lang,
                set.len(),
                list.len()
            );
        }
    }

    #[test]
    fn test_all_lowercase() {
        for lang in SUPPORTED_STOPWORD_LANGS {
            let list = stopword_list_for_lang(lang).expect("expected stopword list");
            for word in list {
                assert_eq!(
                    word,
                    word.to_lowercase(),
                    "Stopword '{}' in '{}' is not lowercase",
                    word,
                    *lang
                );
            }
        }
    }

    #[test]
    fn test_parse_stopword_lines_ignores_comments_and_blank_lines() {
        let words = parse_stopword_lines(
            "# comment\n\
             \n\
             the\n\
               and  \n\
             # trailing comment\n",
        );
        assert_eq!(words, vec!["the", "and"]);
    }
}
