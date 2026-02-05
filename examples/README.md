# Examples

This directory contains example code for using the ML-Audit platform.

## Running Examples

1. Start the ML-Audit server:
```bash
python app/main.py
```

2. In another terminal, run the examples:
```bash
python examples/llm_examples.py
```

## Prerequisites

- ML-Audit server running (http://localhost:8000)
- Anthropic API key configured in `.env` (for LLM examples)

## Available Examples

- `llm_examples.py` - Demonstrates LLM-powered insights features
  - Natural language Q&A
  - Anomaly explanations
  - Recommendation summaries
  - Win-back email generation
  - Full analysis with LLM

## Coming Soon

- `churn_prediction_example.py` - Customer churn prediction workflow
- `anomaly_detection_example.py` - Detecting unusual patterns
- `seo_audit_example.py` - Website SEO analysis
- `data_sync_example.py` - Syncing data from all sources
