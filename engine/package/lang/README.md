# Language Data Bundle

This directory contains compile-time bundled language resources used by the engine query pipeline.

## Layout

- `stopwords/<lang>.txt`: newline-delimited stopword lists per language code.
- `plurals/irregular-plurals-en.json`: English irregular plural dictionary.
- `decompound/de_words_de.txt`: German root-word list for decompound splitting.

## Sources and licenses

- Stopwords: Snowball and other public standard stopword lists collected in `engine/src/query/stopwords/*.rs` during Stage 2 language rollout.
  - These files were exported into `stopwords/*.txt` without changing token content.
- English irregular plurals: sourced from sindresorhus `irregular-plurals` (MIT).
  - Upstream package metadata is retained in `engine/package/`.
- German decompound roots: curated project word list used for deterministic German compound splitting.

## Notes

- Files are embedded with Rust `include_str!()` from `engine/src/query/*`.
- Stopword files support `#` comments and blank lines; parser ignores both.
