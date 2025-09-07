
# Oaktree Variance Explanation Drafts — MVP

1) Ingest monthly Budget vs Actuals (+ Change Orders + Vendor mapping)
2) Compute material variances per project/period/category
3) Draft investor-ready explanations (EN/AR) using a prompt contract
4) Return JSON drafts for analyst review (with evidence links)

The app supports two mutually-exclusive upload modes:

- **Structured track** – provide four CSV/Excel files (budget–actuals, change orders, vendor map, category map).
- **Freeform track** – provide a single CSV/Excel/Word/PDF/text file. The app will attempt to extract rows and totals using deterministic parsing with ChatGPT assistance.

ChatGPT is used both to process uploaded data and to draft the variance explanations, but prompts enforce a strict no‑invention policy so outputs remain grounded in the provided evidence.

## Run
```
pip install fastapi uvicorn pydantic openai
export OPENAI_API_KEY=...   # optional
export OPENAI_MODEL=gpt-5.1-mini
uvicorn app.main:app --reload
```
Docs: `http://localhost:8000/docs`

## Example payload
(see /data/templates for sample CSV/XLSX files and README for example JSON)

## Environment variables
The service exposes several knobs for timeouts and concurrency. All have safe defaults and may be overridden per deployment:

```
OPENAI_TIMEOUT=30       # seconds per OpenAI request
OPENAI_MAX_RETRIES=2    # OpenAI client retries
PDF_PARSE_TIMEOUT=45    # seconds to spend parsing PDFs

WEB_CONCURRENCY=4       # Gunicorn worker processes
WEB_THREADS=8           # threads per worker
WEB_TIMEOUT=240         # request timeout in seconds
WEB_KEEPALIVE=5         # keepalive in seconds
```

### LLM configuration
```
OPENAI_API_KEY or AZURE_OPENAI_API_KEY  # one required for OpenAI calls
OPENAI_BASE_URL                         # optional custom endpoint
OPENAI_MODEL=gpt-4o-mini                # default model
FORCE_LLM=false                         # force OpenAI even if local_only=true
LOCAL_FALLBACK_POLICY=on_error|never|if_no_key
                                        # when to fall back locally
```

### Manual checks
```
# Happy path (OpenAI)
curl -s -X POST http://localhost:8000/drafts/from-file \
  -F file=@tests/fixtures/sample.pdf \
  -F local_only=false | jq '._meta'

# Body flag forces local
curl -s -X POST http://localhost:8000/drafts/from-file \
  -F file=@tests/fixtures/sample.pdf \
  -F local_only=true | jq '._meta'

# Header ignored
curl -s -X POST http://localhost:8000/drafts/from-file \
  -H "x-local-only: true" \
  -F file=@tests/fixtures/sample.pdf | jq '._meta'

# Fallback policy (prod style)
LOCAL_FALLBACK_POLICY=never OPENAI_API_KEY= \
  uvicorn app.main:app  # then call endpoint and expect clear error
```
