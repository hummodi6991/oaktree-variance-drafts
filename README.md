
# Oaktree Variance Explanation Drafts — MVP

1) Ingest monthly Budget vs Actuals (+ Change Orders + Vendor mapping)
2) Compute material variances per project/period/category
3) Draft investor-ready explanations (EN/AR) using a prompt contract
4) Return JSON drafts for analyst review (with evidence links)

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
