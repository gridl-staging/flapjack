// ── French plural rules ──
// French plurals are simpler than English: mostly add -s.
// Key exceptions: -eau → -eaux, -al → -aux, -ou (some) → -oux

/// Expand a French word to all known plural and singular forms.
///
/// First attempts to strip plural suffixes (-eaux, -aux, -oux, -s). If successful, returns the singular form plus the input. Otherwise generates the plural and returns both forms. Returns just the input if no plural form exists.
pub(super) fn expand_plurals_french(word: &str) -> Vec<String> {
    let mut forms = vec![word.to_string()];

    // Try stripping plural → singular
    if let Some(singular) = strip_french_plural(word) {
        if singular != word && !forms.contains(&singular) {
            forms.push(singular);
        }
        return forms;
    }

    // Try generating plural from singular
    let plural = generate_french_plural(word);
    if plural != word && !forms.contains(&plural) {
        forms.push(plural);
    }

    forms
}

/// Strip French plural suffixes to recover the singular form.
///
/// Handles -eaux → -eau, -aux → -al, -oux → -ou, and regular -s. Returns None if no plural suffix is recognized.
fn strip_french_plural(word: &str) -> Option<String> {
    // -eaux → -eau (bateaux → bateau)
    if word.ends_with("eaux") && word.len() > 4 {
        return Some(format!("{}eau", &word[..word.len() - 4]));
    }
    // -aux → -al (chevaux → cheval, journaux → journal)
    if word.ends_with("aux") && word.len() > 3 {
        return Some(format!("{}al", &word[..word.len() - 3]));
    }
    // -oux → -ou (bijoux → bijou, genoux → genou)
    if word.ends_with("oux") && word.len() > 3 {
        return Some(format!("{}ou", &word[..word.len() - 3]));
    }
    // regular -s (chats → chat)
    if word.ends_with('s') && !word.ends_with("ss") && word.len() > 2 {
        return Some(word[..word.len() - 1].to_string());
    }
    None
}

/// Generate the French plural form from a singular word.
///
/// Applies productive French patterns: -eau → -eaux, -al → -aux, -ou exceptions → -oux, -s/-x/-z unchanged, or default -s.
fn generate_french_plural(word: &str) -> String {
    // -eau → -eaux
    if word.ends_with("eau") {
        return format!("{}x", word);
    }
    // -al → -aux (but not all: festival → festivals; we do the common rule)
    if word.ends_with("al") && word.len() > 2 {
        return format!("{}aux", &word[..word.len() - 2]);
    }
    // -ou → -oux for common exceptions (bijou, caillou, chou, genou, hibou, joujou, pou)
    let ou_exceptions = [
        "bijou", "caillou", "chou", "genou", "hibou", "joujou", "pou",
    ];
    if ou_exceptions.contains(&word) {
        return format!("{}x", word);
    }
    // -s, -x, -z unchanged
    if word.ends_with('s') || word.ends_with('x') || word.ends_with('z') {
        return word.to_string();
    }
    // default: add -s
    format!("{}s", word)
}

// ── German plural rules ──
// German has complex plural patterns: -e, -er, -en, -n, -s, umlaut changes.
// Rule-based approach covers the most common patterns.

/// Expand a German word to all known plural and singular forms.
///
/// First attempts to strip plural suffixes (-en, -er, -e, -s, -n). If successful, returns candidate singular forms plus the input. Otherwise generates the plural and returns both forms.
pub(super) fn expand_plurals_german(word: &str) -> Vec<String> {
    let mut forms = vec![word.to_string()];

    let singulars = strip_german_plural(word);
    if !singulars.is_empty() {
        for singular in singulars {
            if singular != word && !forms.contains(&singular) {
                forms.push(singular);
            }
        }
        return forms;
    }

    let plural = generate_german_plural(word);
    if plural != word && !forms.contains(&plural) {
        forms.push(plural);
    }

    forms
}

/// Returns candidate singular forms for a German plural.
/// German -en plurals can strip to either bare stem (Frauen→Frau) or stem+e (Blumen→Blume),
/// so this returns multiple candidates.
fn strip_german_plural(word: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    // -en (Frauen → Frau, Blumen → Blume)
    if word.ends_with("en") && word.len() > 3 {
        let bare_stem = &word[..word.len() - 2]; // strip -en: Frauen → Frau
        candidates.push(bare_stem.to_string());
        let stem_e = format!("{}e", bare_stem); // strip -n: Blumen → Blume
        if stem_e != word {
            candidates.push(stem_e);
        }
        return candidates;
    }
    // -er (Kinder → Kind, Bücher → Buch)
    if word.ends_with("er") && word.len() > 3 {
        candidates.push(word[..word.len() - 2].to_string());
        return candidates;
    }
    // -e (Tage → Tag, Hunde → Hund)
    if word.ends_with('e') && word.len() > 2 {
        candidates.push(word[..word.len() - 1].to_string());
        return candidates;
    }
    // -s (Autos → Auto, Büros → Büro)
    if word.ends_with('s') && word.len() > 2 {
        candidates.push(word[..word.len() - 1].to_string());
        return candidates;
    }
    // -n (Straßen → Straße)
    if word.ends_with('n') && word.len() > 2 {
        candidates.push(word[..word.len() - 1].to_string());
        return candidates;
    }
    candidates
}

fn generate_german_plural(word: &str) -> String {
    // Common German plural patterns (rule-based approximation):
    // Words ending in -e usually add -n
    if word.ends_with('e') {
        return format!("{}n", word);
    }
    // Words ending in -er, -el, -en often unchanged or add -n
    if word.ends_with("er") || word.ends_with("el") {
        return word.to_string(); // Often unchanged in plural
    }
    // Default: add -e (most common German plural suffix)
    format!("{}e", word)
}

// ── Spanish plural rules ──
// Spanish plurals are relatively regular:
// - Vowel endings: add -s
// - Consonant endings: add -es
// - -z → -ces

/// Expand a Spanish word to all known plural and singular forms.
///
/// First attempts to strip plural suffixes (-ces, -es after consonants, -s after vowels). If successful, returns the singular form plus the input. Otherwise generates the plural and returns both forms.
pub(super) fn expand_plurals_spanish(word: &str) -> Vec<String> {
    let mut forms = vec![word.to_string()];

    if let Some(singular) = strip_spanish_plural(word) {
        if singular != word && !forms.contains(&singular) {
            forms.push(singular);
        }
        return forms;
    }

    let plural = generate_spanish_plural(word);
    if plural != word && !forms.contains(&plural) {
        forms.push(plural);
    }

    forms
}

/// Strip Spanish plural suffixes to recover the singular form.
///
/// Handles -ces → -z for short words, -es after consonants, and -s after vowels.
fn strip_spanish_plural(word: &str) -> Option<String> {
    // -ces → -z (luces → luz, peces → pez, voces → voz, jueces → juez, raíces → raíz)
    if word.ends_with("ces") && word.len() > 3 && word.chars().count() <= 6 {
        return Some(format!("{}z", &word[..word.len() - 3]));
    }
    // -es after consonant (ciudades → ciudad, canciones → canción)
    if word.ends_with("es") && word.len() > 3 {
        let stem = &word[..word.len() - 2];
        let last_byte = stem.as_bytes().last().copied().unwrap_or(0);
        // If stem ends in consonant, this was -es plural
        if !matches!(last_byte, b'a' | b'e' | b'i' | b'o' | b'u') {
            return Some(stem.to_string());
        }
    }
    // -s after vowel (casas → casa, libros → libro)
    if word.ends_with('s') && word.len() > 2 {
        let before_s = word.as_bytes()[word.len() - 2];
        if matches!(before_s, b'a' | b'e' | b'i' | b'o' | b'u') {
            return Some(word[..word.len() - 1].to_string());
        }
    }
    None
}

fn generate_spanish_plural(word: &str) -> String {
    // -z → -ces
    if let Some(stem) = word.strip_suffix('z') {
        return format!("{}ces", stem);
    }
    // Vowel ending: add -s
    let last_byte = word.as_bytes().last().copied().unwrap_or(0);
    if matches!(last_byte, b'a' | b'e' | b'i' | b'o' | b'u') {
        return format!("{}s", word);
    }
    // Consonant ending: add -es
    format!("{}es", word)
}

// ── Portuguese plural rules ──
// Rule-based approximation for common productive patterns.

/// TODO: Document is_portuguese_vowel.
fn is_portuguese_vowel(ch: char) -> bool {
    matches!(
        ch,
        'a' | 'e'
            | 'i'
            | 'o'
            | 'u'
            | 'á'
            | 'à'
            | 'â'
            | 'ã'
            | 'é'
            | 'ê'
            | 'í'
            | 'ó'
            | 'ô'
            | 'õ'
            | 'ú'
    )
}

/// Expand a Portuguese word to all known plural and singular forms.
///
/// First attempts to strip plural suffixes. If successful, returns the singular form plus the input. Otherwise generates the plural from singular rules and returns both forms.
pub(super) fn expand_plurals_portuguese(word: &str) -> Vec<String> {
    let mut forms = vec![word.to_string()];

    if let Some(singular) = strip_portuguese_plural(word) {
        if singular != word && !forms.contains(&singular) {
            forms.push(singular);
        }
        return forms;
    }

    let plural = generate_portuguese_plural(word);
    if plural != word && !forms.contains(&plural) {
        forms.push(plural);
    }

    forms
}

/// Strip Portuguese plural suffixes to recover the singular form.
fn strip_portuguese_plural(word: &str) -> Option<String> {
    // Common invariable words ending with -is.
    let invariants = ["lápis", "ônibus", "pires", "vírus"];
    if invariants.contains(&word) {
        return None;
    }
    if let Some(stem) = word.strip_suffix("ões") {
        return Some(format!("{stem}ão"));
    }
    if let Some(stem) = word.strip_suffix("ães") {
        return Some(format!("{stem}ão"));
    }
    if let Some(stem) = word.strip_suffix("ais") {
        return Some(format!("{stem}al"));
    }
    if let Some(stem) = word.strip_suffix("éis") {
        return Some(format!("{stem}el"));
    }
    if let Some(stem) = word.strip_suffix("óis") {
        return Some(format!("{stem}ol"));
    }
    if word.ends_with('s') && !word.ends_with("is") && word.chars().count() > 2 {
        let mut chars = word.chars();
        chars.next_back();
        if let Some(before_s) = chars.next_back() {
            if is_portuguese_vowel(before_s) {
                return Some(word[..word.len() - 1].to_string());
            }
        }
    }
    None
}

/// Generate the Portuguese plural form from a singular word.
fn generate_portuguese_plural(word: &str) -> String {
    if let Some(stem) = word.strip_suffix("ão") {
        return format!("{stem}ões");
    }
    if let Some(stem) = word.strip_suffix("al") {
        return format!("{stem}ais");
    }
    if let Some(stem) = word.strip_suffix("el") {
        return format!("{stem}éis");
    }
    if let Some(stem) = word.strip_suffix("ol") {
        return format!("{stem}óis");
    }
    if word.ends_with('s') {
        return word.to_string();
    }
    if let Some(last) = word.chars().last() {
        if is_portuguese_vowel(last) {
            return format!("{}s", word);
        }
    }
    format!("{}es", word)
}

// ── Italian plural rules ──
// Rule-based approximation for common patterns.

/// Expand an Italian word to all known plural and singular forms.
pub(super) fn expand_plurals_italian(word: &str) -> Vec<String> {
    let mut forms = vec![word.to_string()];

    let singulars = strip_italian_plural(word);
    if !singulars.is_empty() {
        for singular in singulars {
            if singular != word && !forms.contains(&singular) {
                forms.push(singular);
            }
        }
        return forms;
    }

    let plural = generate_italian_plural(word);
    if plural != word && !forms.contains(&plural) {
        forms.push(plural);
    }

    forms
}

/// Strip Italian plural suffixes to recover candidate singular forms.
fn strip_italian_plural(word: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    if word.ends_with("chi") && word.chars().count() > 3 {
        candidates.push(format!("{}co", &word[..word.len() - 3]));
        return candidates;
    }
    if word.ends_with("he") && word.chars().count() > 2 {
        candidates.push(format!("{}a", &word[..word.len() - 2]));
        return candidates;
    }
    if word.ends_with('i') && word.chars().count() > 2 {
        candidates.push(format!("{}o", &word[..word.len() - 1]));
        candidates.push(format!("{}e", &word[..word.len() - 1]));
        return candidates;
    }
    if word.ends_with('e') && word.chars().count() > 2 {
        candidates.push(format!("{}a", &word[..word.len() - 1]));
        return candidates;
    }
    if word.ends_with('s') && word.chars().count() > 2 {
        candidates.push(word[..word.len() - 1].to_string());
        return candidates;
    }
    candidates
}

/// Generate the Italian plural form from a singular word.
fn generate_italian_plural(word: &str) -> String {
    if word.ends_with('s') {
        return word.to_string();
    }
    if word.ends_with("co") && word.chars().count() > 2 {
        return format!("{}chi", &word[..word.len() - 2]);
    }
    if word.ends_with("ca") && word.chars().count() > 2 {
        return format!("{}che", &word[..word.len() - 2]);
    }
    if word.ends_with("ga") && word.chars().count() > 2 {
        return format!("{}ghe", &word[..word.len() - 2]);
    }
    if let Some(stem) = word.strip_suffix('o') {
        return format!("{stem}i");
    }
    if let Some(stem) = word.strip_suffix('a') {
        return format!("{stem}e");
    }
    if let Some(stem) = word.strip_suffix('e') {
        return format!("{stem}i");
    }
    format!("{}s", word)
}

// ── Dutch plural rules ──
// Rule-based approximation for productive forms.

fn is_dutch_vowel(ch: char) -> bool {
    matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y')
}

/// Expand a Dutch word to all known plural and singular forms.
pub(super) fn expand_plurals_dutch(word: &str) -> Vec<String> {
    let mut forms = vec![word.to_string()];

    if let Some(singular) = strip_dutch_plural(word) {
        if singular != word && !forms.contains(&singular) {
            forms.push(singular);
        }
        return forms;
    }

    let plural = generate_dutch_plural(word);
    if plural != word && !forms.contains(&plural) {
        forms.push(plural);
    }

    forms
}

fn strip_dutch_plural(word: &str) -> Option<String> {
    if word.ends_with("'s") && word.chars().count() > 2 {
        return Some(word[..word.len() - 2].to_string());
    }
    if word.ends_with("en") && word.chars().count() > 3 {
        return Some(word[..word.len() - 2].to_string());
    }
    if word.ends_with('s') && word.chars().count() > 2 {
        return Some(word[..word.len() - 1].to_string());
    }
    None
}

/// Generate the Dutch plural form from a singular word.
fn generate_dutch_plural(word: &str) -> String {
    if let Some(last) = word.chars().last() {
        if is_dutch_vowel(last) {
            return format!("{}'s", word);
        }
    }
    if word.ends_with("el")
        || word.ends_with("er")
        || word.ends_with("en")
        || word.ends_with("em")
        || word.ends_with("aar")
    {
        return format!("{}s", word);
    }
    format!("{}en", word)
}
