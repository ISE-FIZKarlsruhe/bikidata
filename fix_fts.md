In `bikidata`, the Tantivy schema collapses all literals for an entity into three language-blind buckets (`label`, `other`, and `linked`—or weighted tiers of labels, literals, and unrolled blank nodes/linked IRIs) and runs a **single global stemmer** across the entire index (e.g., `build_fts(stemmer="porter")` or `"german"`).

When an RDF knowledge graph contains multilingual literals (`"The Night Watch"@en`, `"De Nachtwacht"@nl`, `"Die Nachtwache"@de`), a single global stemmer causes two major problems:

1. **Cross-Language Stemming Corruption:** Feeding Dutch or German words into an English Porter stemmer (or vice versa) mangles word endings, causing false-positive collisions or failing to match valid inflections.
2. **IDF & Length-Normalization Skew:** Mixing all languages in one field skews BM25 Inverse Document Frequency (IDF) and document length (`dl` / `avgdl`) calculations—entities with translations in 15 languages look like "longer" documents than entities only labeled in Dutch, penalizing matches on the heavily translated entity.

To fix this in Tantivy (via `tantivy-py`), you should split your schema by **both weight tier and language tag**, backed by a language-agnostic fallback field.

---

## 1. The Recommended Schema Architecture: Per-Language Tiered Fields + Fallback

Because Tantivy binds tokenizers at the **field level** (not per-value or per-document), handling multiple languages properly requires routing RDF `@lang` tags into dedicated language fields while keeping a folded, unstemmed catch-all field for untagged literals (`xsd:string`), proper nouns, and minor languages.

| Field Pattern | Target RDF Literals | Tokenizer Pipeline | Role in Querying |
| --- | --- | --- | --- |
| `label_{lang}` (e.g., `label_en`, `label_nl`) | `rdfs:label`, `skos:prefLabel`, `schema:name` with `@lang` | `simple` -> `remove_long(40)` -> `lowercase` -> `ascii_fold` -> `stemmer(lang)` | Highest boost (e.g., `^5.0`). Matches morphological variations in that specific language. |
| `text_{lang}` (e.g., `text_en`, `text_nl`) | Descriptions, notes, blank node literals, linked labels with `@lang` | Same language-specific analyzer as `label_{lang}` | Base weight (`^1.0`). Captures full-text body per language with accurate BM25 stats. |
| `label_all` | **All** primary labels (regardless of `@lang`, plus untagged literals) | `simple` -> `remove_long(40)` -> `lowercase` -> `ascii_fold` *(No stemmer)* | Medium-high boost (`^3.0`). Guarantees exact/folded matches on proper names (e.g., *"Dürer"* -> *"durer"*) and languages without a dedicated stemmer. |
| `text_all` | **All** secondary/linked literals (tagged and untagged) | `simple` -> `remove_long(40)` -> `lowercase` -> `ascii_fold` *(No stemmer)* | Low boost (`^0.5`). Catch-all recall across the graph. |
| `langs` (Facet / Fast Field) | The distinct `@lang` tags present on the entity | `raw` (or `Facet`) | Allows instant filtering of entities that actually have labels/descriptions in a user's UI language. |

---

## 2. Implementing the Schema & Custom Analyzers in `tantivy-py`

Tantivy's built-in Snowball stemmer supports 18+ languages (including English, Dutch, German, French, Italian, Spanish, Portuguese, Russian, Swedish, etc.). You can construct and register these pipelines dynamically using `TextAnalyzerBuilder`:

```python
import tantivy
from tantivy import SchemaBuilder, TextAnalyzerBuilder, Tokenizer, Filter

# Map RDF BCP-47 @lang tags to Tantivy's Snowball language names
LANG_MAP = {
    "en": "english",
    "nl": "dutch",
    "de": "german",
    "fr": "french",
    "es": "spanish",
    "it": "italian",
}

def build_multilingual_index(index_path: str):
    builder = SchemaBuilder()
    
    # 1. Core identifier field
    builder.add_text_field("iri", stored=True, tokenizer_name="raw")
    builder.add_text_field("langs", stored=False, tokenizer_name="raw")
    
    # 2. Language-agnostic fallback fields (folded, unstemmed)
    builder.add_text_field("label_all", stored=False, tokenizer_name="folded_nostem")
    builder.add_text_field("text_all", stored=False, tokenizer_name="folded_nostem")
    
    # 3. Language-specific fields for primary languages in your KG
    for iso_code in LANG_MAP:
        tok_name = f"stem_{iso_code}"
        builder.add_text_field(f"label_{iso_code}", stored=False, tokenizer_name=tok_name)
        builder.add_text_field(f"text_{iso_code}", stored=False, tokenizer_name=tok_name)

    schema = builder.build()
    index = tantivy.Index(schema, path=index_path)

    # Register the language-agnostic ASCII-folding analyzer
    folded_analyzer = (
        TextAnalyzerBuilder(Tokenizer.simple())
        .filter(Filter.remove_long(40))
        .filter(Filter.lowercase())
        .filter(Filter.ascii_fold())
        .build()
    )
    index.register_tokenizer("folded_nostem", folded_analyzer)

    # Register per-language stemmed analyzers
    for iso_code, snowball_lang in LANG_MAP.items():
        lang_analyzer = (
            TextAnalyzerBuilder(Tokenizer.simple())
            .filter(Filter.remove_long(40))
            .filter(Filter.lowercase())
            .filter(Filter.ascii_fold())
            .filter(Filter.stopword(snowball_lang))
            .filter(Filter.stemmer(snowball_lang))
            .build()
        )
        index.register_tokenizer(f"stem_{iso_code}", lang_analyzer)

    return index

```

> **Why `Filter.ascii_fold()` before `Filter.stemmer()` matters in a KG:** Users frequently type queries without diacritics (e.g., searching `"munster"` or `"françois"` without the cedilla). Adding `ascii_fold()` into both the fallback and language-specific pipelines ensures diacritic-insensitive matching while still preserving stemmer recall.

---

## 3. Routing RDF Literals During Indexing

When extracting literals from your triple store (or DuckDB table as `bikidata` does), normalize the RDF language tag (e.g., stripping regional subtags like `@en-GB` -> `en` or `@nl-BE` -> `nl`):

```python
def index_entity_literals(doc: tantivy.Document, iri: str, literals: list[tuple[str, str | None, bool]]):
    """
    literals: list of (value, lang_tag, is_label)
    """
    doc.add_text("iri", iri)
    seen_langs = set()

    for value, lang, is_label in literals:
        # Always populate the folded, unstemmed catch-all fields
        fallback_field = "label_all" if is_label else "text_all"
        doc.add_text(fallback_field, value)

        if lang:
            base_lang = lang.split("-")[0].lower()
            seen_langs.add(base_lang)
            if base_lang in LANG_MAP:
                target_field = f"label_{base_lang}" if is_label else f"text_{base_lang}"
                doc.add_text(target_field, value)

    for l in seen_langs:
        doc.add_text("langs", l)

```

---

## 4. Query-Time Strategy: Known Language vs. Language-Agnostic Search

Splitting the schema gives you two clean query modes using `index.parse_query()`:

1. **User/Query Language is Known (e.g., `lang="nl"`):**
Target the Dutch stemmed fields with high boosts, while keeping `label_all` and `text_all` at a lower boost so untranslated proper nouns, Latin mottos, or English technical terms on Dutch entities still match:
```python
query = index.parse_query(
    user_query,
    default_field_names=["label_nl", "text_nl", "label_all", "text_all"],
    field_boosts={"label_nl": 10.0, "label_all": 5.0, "text_nl": 2.0, "text_all": 0.5}
)

```


2. **Language-Agnostic / Cross-Lingual Search (`lang=None`):**
Query across `label_all`, `text_all`, and all active `label_{lang}` fields simultaneously. Because Tantivy's `parse_query` automatically applies each field's registered analyzer to the query string for that specific field's sub-query, a single search term is stemmed in Dutch against `label_nl`, stemmed in German against `label_de`, and matched exact/folded against `label_all` in one pass.


