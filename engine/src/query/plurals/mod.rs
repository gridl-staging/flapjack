//! Multilingual plural expansion module supporting English, French, German, Spanish, Portuguese, Italian, and Dutch with rule-based and dictionary-driven pluralization.

mod languages;

use languages::*;
use std::collections::HashMap;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Default)]
pub enum IgnorePluralsValue {
    #[default]
    Disabled,
    All,
    Languages(Vec<String>),
}

impl serde::Serialize for IgnorePluralsValue {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            IgnorePluralsValue::Disabled => serializer.serialize_bool(false),
            IgnorePluralsValue::All => serializer.serialize_bool(true),
            IgnorePluralsValue::Languages(langs) => langs.serialize(serializer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for IgnorePluralsValue {
    /// Deserialize a plural expansion setting from JSON: boolean or array of language codes.
    ///
    /// Converts `true` to All, `false` to Disabled, empty array to Disabled, and non-empty array to Languages(langs).
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de;

        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = IgnorePluralsValue;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("bool or array of language codes")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Self::Value, E> {
                if v {
                    Ok(IgnorePluralsValue::All)
                } else {
                    Ok(IgnorePluralsValue::Disabled)
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
                    Ok(IgnorePluralsValue::Disabled)
                } else {
                    Ok(IgnorePluralsValue::Languages(langs))
                }
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

static DICTIONARY: OnceLock<HashMap<String, Vec<String>>> = OnceLock::new();

/// Load the irregular plurals dictionary from the bundled JSON resource.
///
/// Reads irregular-plurals-en.json and builds a bidirectional mapping: each word maps to all its known singular/plural alternates (e.g., child→[children], children→[child]).
fn load_dictionary() -> HashMap<String, Vec<String>> {
    let json_str = include_str!("../../../package/lang/plurals/irregular-plurals-en.json");
    let raw: HashMap<String, String> =
        serde_json::from_str(json_str).expect("invalid irregular-plurals-en.json");

    let mut bidir: HashMap<String, Vec<String>> = HashMap::new();

    for (singular, plural) in &raw {
        let s = singular.to_lowercase();
        let p = plural.to_lowercase();

        if s == p {
            bidir.entry(s).or_default();
            continue;
        }

        bidir.entry(s.clone()).or_default();
        if !bidir[&s].contains(&p) {
            bidir.get_mut(&s).unwrap().push(p.clone());
        }

        bidir.entry(p.clone()).or_default();
        if !bidir[&p].contains(&s) {
            bidir.get_mut(&p).unwrap().push(s.clone());
        }
    }

    bidir
}

fn get_dictionary() -> &'static HashMap<String, Vec<String>> {
    DICTIONARY.get_or_init(load_dictionary)
}

/// Strip common English plural suffixes to recover the singular form.
///
/// Handles -ies (batteries→battery), -es after consonants or sibilants (churches→church, boxes→box), and -s after non-special consonants (cats→cat). Returns None if the word doesn't match a regular plural pattern.
fn strip_regular_plural(word: &str) -> Option<String> {
    if word.ends_with("ies") && word.len() > 4 {
        let before = word.as_bytes()[word.len() - 4];
        if !matches!(before, b'a' | b'e' | b'i' | b'o' | b'u') {
            return Some(format!("{}y", &word[..word.len() - 3]));
        }
    }

    if word.ends_with("sses")
        || word.ends_with("ches")
        || word.ends_with("shes")
        || word.ends_with("xes")
        || word.ends_with("zes")
    {
        return Some(word[..word.len() - 2].to_string());
    }

    if word.ends_with('s')
        && !word.ends_with("ss")
        && !word.ends_with("us")
        && !word.ends_with("is")
        && word.len() > 2
    {
        return Some(word[..word.len() - 1].to_string());
    }

    None
}

/// Generate the English plural form from a singular word using productive rules.
///
/// Handles y→ies (battery→batteries) for consonant-before-y, -s/-x/-z/-ch/-sh → add -es, and default add -s.
fn generate_regular_plural(word: &str) -> String {
    if word.ends_with('y') && word.len() > 2 {
        let before_y = word.as_bytes()[word.len() - 2];
        if !matches!(before_y, b'a' | b'e' | b'i' | b'o' | b'u') {
            return format!("{}ies", &word[..word.len() - 1]);
        }
    }
    if word.ends_with("sh")
        || word.ends_with("ch")
        || word.ends_with('s')
        || word.ends_with('x')
        || word.ends_with('z')
    {
        return format!("{}es", word);
    }
    format!("{}s", word)
}

/// Expand an English word to all known plural and singular forms using dictionary + rules.
///
/// First checks the irregular plurals dictionary for bidirectional forms. Falls back to stripping regular plurals (batteries→battery). Finally generates regular plurals (car→cars) if no known form exists. Always returns at least the lowercase input word.
pub fn expand_plurals(word: &str) -> Vec<String> {
    let lower = word.to_lowercase();
    let mut forms = vec![lower.clone()];

    let dict = get_dictionary();
    if let Some(others) = dict.get(lower.as_str()) {
        if others.is_empty() {
            return forms;
        }
        for other in others {
            if !forms.contains(other) {
                forms.push(other.clone());
            }
        }
        return forms;
    }

    if let Some(singular) = strip_regular_plural(&lower) {
        if singular != lower && !forms.contains(&singular) {
            if let Some(dict_others) = dict.get(singular.as_str()) {
                if dict_others.is_empty() {
                    return forms;
                }
            }
            forms.push(singular);
        }
        return forms;
    }

    let plural = generate_regular_plural(&lower);
    if plural != lower && !forms.contains(&plural) {
        forms.push(plural);
    }

    forms
}

/// Determine which languages should have plural expansion applied based on configuration and query context.
pub fn resolve_plural_languages(
    ignore_plurals: &IgnorePluralsValue,
    query_languages: &[String],
) -> Vec<String> {
    match ignore_plurals {
        IgnorePluralsValue::Disabled => vec![],
        IgnorePluralsValue::Languages(langs) => langs.clone(),
        IgnorePluralsValue::All => {
            if query_languages.is_empty() {
                vec!["en".to_string()]
            } else {
                query_languages.to_vec()
            }
        }
    }
}

trait PluralProvider {
    fn expand(&self, word: &str) -> Vec<String>;
}

struct EnglishPluralProvider;
struct FrenchPluralProvider;
struct GermanPluralProvider;
struct SpanishPluralProvider;
struct PortuguesePluralProvider;
struct ItalianPluralProvider;
struct DutchPluralProvider;

impl PluralProvider for EnglishPluralProvider {
    fn expand(&self, word: &str) -> Vec<String> {
        expand_plurals(word)
    }
}

impl PluralProvider for FrenchPluralProvider {
    fn expand(&self, word: &str) -> Vec<String> {
        expand_plurals_french(word)
    }
}

impl PluralProvider for GermanPluralProvider {
    fn expand(&self, word: &str) -> Vec<String> {
        expand_plurals_german(word)
    }
}

impl PluralProvider for SpanishPluralProvider {
    fn expand(&self, word: &str) -> Vec<String> {
        expand_plurals_spanish(word)
    }
}

impl PluralProvider for PortuguesePluralProvider {
    fn expand(&self, word: &str) -> Vec<String> {
        expand_plurals_portuguese(word)
    }
}

impl PluralProvider for ItalianPluralProvider {
    fn expand(&self, word: &str) -> Vec<String> {
        expand_plurals_italian(word)
    }
}

impl PluralProvider for DutchPluralProvider {
    fn expand(&self, word: &str) -> Vec<String> {
        expand_plurals_dutch(word)
    }
}

static ENGLISH_PLURAL_PROVIDER: EnglishPluralProvider = EnglishPluralProvider;
static FRENCH_PLURAL_PROVIDER: FrenchPluralProvider = FrenchPluralProvider;
static GERMAN_PLURAL_PROVIDER: GermanPluralProvider = GermanPluralProvider;
static SPANISH_PLURAL_PROVIDER: SpanishPluralProvider = SpanishPluralProvider;
static PORTUGUESE_PLURAL_PROVIDER: PortuguesePluralProvider = PortuguesePluralProvider;
static ITALIAN_PLURAL_PROVIDER: ItalianPluralProvider = ItalianPluralProvider;
static DUTCH_PLURAL_PROVIDER: DutchPluralProvider = DutchPluralProvider;

fn normalized_plural_lang(lang: &str) -> String {
    lang.trim().to_ascii_lowercase()
}

fn plural_provider_for_lang(lang: &str) -> Option<&'static dyn PluralProvider> {
    match normalized_plural_lang(lang).as_str() {
        "en" => Some(&ENGLISH_PLURAL_PROVIDER),
        "fr" => Some(&FRENCH_PLURAL_PROVIDER),
        "de" => Some(&GERMAN_PLURAL_PROVIDER),
        "es" => Some(&SPANISH_PLURAL_PROVIDER),
        "pt" | "pt-br" => Some(&PORTUGUESE_PLURAL_PROVIDER),
        "it" => Some(&ITALIAN_PLURAL_PROVIDER),
        "nl" => Some(&DUTCH_PLURAL_PROVIDER),
        _ => None,
    }
}

/// Returns true if the given language has plural expansion support.
pub fn has_plural_support(lang: &str) -> bool {
    plural_provider_for_lang(lang).is_some()
}

/// Expand plurals for a specific language. Returns the word plus any plural/singular forms.
/// Falls through gracefully for unsupported languages (returns just the input word).
pub fn expand_plurals_for_lang(word: &str, lang: &str) -> Vec<String> {
    let lower_word = word.to_lowercase();
    plural_provider_for_lang(lang)
        .map(|provider| provider.expand(&lower_word))
        .unwrap_or_else(|| vec![lower_word])
}

/// Expand plurals across multiple languages, merging all forms.
pub fn expand_plurals_multi(word: &str, langs: &[String]) -> Vec<String> {
    let lower = word.to_lowercase();
    let mut forms = vec![lower.clone()];
    for lang in langs {
        let lang_forms = expand_plurals_for_lang(&lower, lang);
        for f in lang_forms {
            if !forms.contains(&f) {
                forms.push(f);
            }
        }
    }
    forms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignore_plurals_bool_round_trips() {
        let enabled: IgnorePluralsValue = serde_json::from_str("true").unwrap();
        let disabled: IgnorePluralsValue = serde_json::from_str("false").unwrap();

        assert_eq!(enabled, IgnorePluralsValue::All);
        assert_eq!(disabled, IgnorePluralsValue::Disabled);
        assert_eq!(serde_json::to_string(&enabled).unwrap(), "true");
        assert_eq!(serde_json::to_string(&disabled).unwrap(), "false");
    }

    #[test]
    fn ignore_plurals_languages_round_trip() {
        let value: IgnorePluralsValue = serde_json::from_str(r#"["en","fr"]"#).unwrap();

        assert_eq!(
            value,
            IgnorePluralsValue::Languages(vec!["en".to_string(), "fr".to_string()])
        );
        assert_eq!(serde_json::to_string(&value).unwrap(), r#"["en","fr"]"#);
    }

    #[test]
    fn resolve_plural_languages_defaults_english_when_query_languages_missing() {
        assert_eq!(
            resolve_plural_languages(&IgnorePluralsValue::All, &[]),
            vec!["en".to_string()]
        );
    }

    #[test]
    fn resolve_plural_languages_uses_explicit_language_list() {
        assert_eq!(
            resolve_plural_languages(
                &IgnorePluralsValue::Languages(vec!["fr".to_string(), "pt-br".to_string()]),
                &["en".to_string()]
            ),
            vec!["fr".to_string(), "pt-br".to_string()]
        );
    }

    #[test]
    fn expand_plurals_covers_regular_and_irregular_english_forms() {
        let regular = expand_plurals("car");
        let irregular = expand_plurals("children");

        assert!(regular.contains(&"car".to_string()));
        assert!(regular.contains(&"cars".to_string()));
        assert!(irregular.contains(&"children".to_string()));
        assert!(irregular.contains(&"child".to_string()));
    }

    #[test]
    fn has_plural_support_is_case_and_whitespace_insensitive() {
        assert!(has_plural_support(" EN "));
        assert!(has_plural_support("\tpt-br\n"));
        assert!(!has_plural_support("ja"));
    }

    #[test]
    fn expand_plurals_for_lang_routes_pt_br_to_portuguese_rules() {
        let forms = expand_plurals_for_lang("ação", " pt-br ");

        assert!(forms.contains(&"ação".to_string()));
        assert!(forms.contains(&"ações".to_string()));
    }

    #[test]
    fn expand_plurals_multi_merges_forms_without_duplicates() {
        let forms = expand_plurals_multi("chat", &["en".to_string(), "fr".to_string()]);

        assert!(forms.contains(&"chat".to_string()));
        assert!(forms.contains(&"chats".to_string()));
        assert_eq!(
            forms.iter().filter(|form| form.as_str() == "chats").count(),
            1
        );
    }
}
