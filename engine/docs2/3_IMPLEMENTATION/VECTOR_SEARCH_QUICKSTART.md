# Vector Search Quickstart

This guide covers building Flapjack with vector search support, verifying the
capability, configuring embedders, and running your first hybrid search query.

> Prerequisite: Flapjack should already be running with basic text search
> working.

## 1. Build with vector search

Vector search requires the `vector-search` feature flag at compile time:

```bash
cd engine
cargo build -p flapjack-server --release --features vector-search
```

For local embedding with FastEmbed, add the `vector-search-local` flag:

```bash
cargo build -p flapjack-server --release --features vector-search-local
```

`vector-search-local` implies `vector-search`.

## 2. Verify capabilities

After starting the server, check `/health`:

```bash
curl -s http://127.0.0.1:7700/health | jq '.capabilities'
```

Expected output with `--features vector-search`:

```json
{
  "vectorSearch": true,
  "vectorSearchLocal": false
}
```

Expected output with `--features vector-search-local`:

```json
{
  "vectorSearch": true,
  "vectorSearchLocal": true
}
```

## 3. Configure an embedder

Embedders are configured per index via the settings API.

### Option A: User-provided vectors

```bash
curl -s -X PUT http://127.0.0.1:7700/1/indexes/products/settings \
  -H "x-algolia-api-key: $FLAPJACK_ADMIN_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{
    "embedders": {
      "default": {
        "source": "userProvided",
        "dimensions": 3
      }
    }
  }'
```

### Option B: OpenAI embeddings

```bash
curl -s -X PUT http://127.0.0.1:7700/1/indexes/products/settings \
  -H "x-algolia-api-key: $FLAPJACK_ADMIN_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{
    "embedders": {
      "default": {
        "source": "openAi",
        "apiKey": "sk-...",
        "model": "text-embedding-3-small",
        "dimensions": 1536,
        "documentTemplate": "A product named {{doc.name}}: {{doc.description}}"
      }
    }
  }'
```

### Option C: Custom REST endpoint

```bash
curl -s -X PUT http://127.0.0.1:7700/1/indexes/products/settings \
  -H "x-algolia-api-key: $FLAPJACK_ADMIN_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{
    "embedders": {
      "default": {
        "source": "rest",
        "url": "http://localhost:8080/embed",
        "dimensions": 384,
        "request": { "input": "{{text}}" },
        "response": { "embedding": "{{embedding}}" },
        "headers": { "Authorization": "Bearer my-token" },
        "documentTemplate": "{{doc.name}} {{doc.description}}"
      }
    }
  }'
```

### Option D: Local embedding with FastEmbed

Requires a binary built with `--features vector-search-local`.

```bash
curl -s -X PUT http://127.0.0.1:7700/1/indexes/products/settings \
  -H "x-algolia-api-key: $FLAPJACK_ADMIN_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{
    "embedders": {
      "default": {
        "source": "fastEmbed",
        "model": "bge-small-en-v1.5",
        "documentTemplate": "{{doc.name}} {{doc.description}}"
      }
    }
  }'
```

## 4. Index documents

With user-provided vectors, include `_vectors` in the document body:

```bash
curl -s -X POST http://127.0.0.1:7700/1/indexes/products/batch \
  -H "x-algolia-api-key: $FLAPJACK_ADMIN_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{
    "requests": [
      {
        "action": "addObject",
        "body": {
          "objectID": "1",
          "name": "Mechanical Keyboard",
          "description": "Cherry MX switches, full-size layout",
          "_vectors": { "default": [0.1, 0.8, 0.3] }
        }
      }
    ]
  }'
```

For OpenAI, REST, and FastEmbed sources, omit `_vectors` and let Flapjack
generate embeddings from the configured template.

## 5. Run a hybrid search

```bash
curl -s -X POST http://127.0.0.1:7700/1/indexes/products/query \
  -H "x-algolia-api-key: $SEARCH_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{
    "query": "keyboard",
    "hybrid": {
      "semanticRatio": 0.5,
      "embedder": "default"
    }
  }'
```

- `semanticRatio: 0.0` means pure keyword
- `semanticRatio: 1.0` means pure vector
- `0.5` is a balanced default

You can also enable `neuralSearch` mode at the index level:

```bash
curl -s -X PUT http://127.0.0.1:7700/1/indexes/products/settings \
  -H "x-algolia-api-key: $FLAPJACK_ADMIN_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{"mode": "neuralSearch"}'
```

## 6. Chat / RAG

`POST /1/indexes/{indexName}/chat` requires:

1. the index in `neuralSearch` mode
2. an embedder configured on the index
3. an AI provider configured either in index settings or via environment
   variables documented in [OPS_CONFIGURATION.md](OPS_CONFIGURATION.md)

Example:

```bash
curl -s -X POST http://127.0.0.1:7700/1/indexes/products/chat \
  -H "x-algolia-api-key: $SEARCH_KEY" \
  -H "x-algolia-application-id: local" \
  -H "content-type: application/json" \
  -d '{"query": "What keyboard do you recommend?"}'
```

## 7. Source anchors

The behavior in this quickstart is grounded in:

- `engine/src/vector/`
- `engine/src/index/settings.rs`
- `engine/flapjack-http/src/handlers/health.rs`
- `engine/flapjack-http/src/dto.rs`
- `engine/flapjack-http/src/handlers/chat.rs`
- `engine/Dockerfile`
