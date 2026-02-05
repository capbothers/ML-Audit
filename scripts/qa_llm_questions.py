#!/usr/bin/env python3
"""
LLM QA runner for business questions.

Runs a list of questions through the same ChatDataService + LLMService pipeline
as the /llm/chat endpoint and ./q CLI, then outputs a pass/fail report.

Usage:
  python scripts/qa_llm_questions.py
  python scripts/qa_llm_questions.py --delay 1.5 --out data/qa_report.json
  python scripts/qa_llm_questions.py --file data/questions.txt
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")

from app.services.chat_data_service import ChatDataService
from app.services.llm_service import LLMService

DEFAULT_QUESTIONS = [
    # Shopify Core
    "Total sales last 30 days",
    "Sales by channel from 2025-12-29 to 2026-01-26",
    "Average order value last 30 days",
    "How many orders in the last 30 days?",
    "How many orders are unfulfilled, partially fulfilled, and cancelled from 2025-12-29 to 2026-01-26?",
    "How much revenue came from discounted orders in the last 30 days?",
    "Which discount codes are being used most in the last 30 days?",
    "How many refunded orders since 2025-02-06?",
    "Returns by product from 2025-12-29 to 2026-01-26",
    "Returns by product category from 2025-12-29 to 2026-01-26",
    "Shipping charges and tax collections over the last 30 days",
    "Top 10 product variants by units in the last 30 days",
    "Lowest selling products in the last 30 days",
    "New vs returning customers in the last 30 days",
    "Top 5 brands by revenue in the last 30 days",

    # GA4
    "Sessions, users, and revenue last 7 days",
    "Top landing pages by sessions in the last 28 days",
    "Top pages by pageviews in the last 28 days",
    "Device breakdown of conversions in the last 28 days",
    "Geo revenue for Australia only (last 28 days)",
    "GA4 ecommerce summary for the last 28 days",
    "Compare GA4 purchases to Shopify orders (last 28 days)",
    "Top products by GA4 revenue in the last 28 days",

    # Search Console
    "Top 10 non-brand queries by clicks in the last 28 days",
    "Top brand queries by clicks in the last 28 days",
    "Queries with CTR < 1% and impressions > 5000",
    "Which queries had the biggest CTR gains week-over-week (last 7 days vs prior 7 days)?",
    "Which pages lost the most clicks vs the prior 28 days?",
    "Earliest and latest Search Console date in the DB?",

    # Caprice / Pricing Intel
    "Who are we following on SKU H57783Z07AU?",
    "What’s the nett nett cost & minimum for SKU HSNRT80B?",
    "Which Zip SKUs are unmatchable?",
    "Which competitors undercut us the most in the last 30 days?",
    "Which brands are undercut the most?",
    "Which SKUs have Do Not Follow set?",
    "Which SKUs have Set Price set?",
    "Which Set Price SKUs are below minimum margin?",
    "Between Dec 15 and Jan 13, who is the competitor at 1799 for SKU HSNRT80B?",

    # NETT Master / Pricing Rules
    "What is the brand cost summary for Zip?",
    "How much are we making at the current price for SKU HSNRT80B?",

    # Inventory
    "Which products are low in stock or out of stock?",
    "Which vendors have the highest inventory value?",
    "What is our inventory turnover rate for the last 30 days?",
]

FAIL_PATTERNS = [
    "i don't see", "i do not see", "i cannot provide", "cannot provide",
    "not available", "no data", "error processing question", "error answering question",
    "not in the provided context", "need access", "would need",
]


def load_questions(path: Path) -> list[str]:
    questions = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        questions.append(line)
    return questions


def is_fail(answer: str) -> bool:
    lowered = answer.lower()
    return any(pat in lowered for pat in FAIL_PATTERNS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LLM QA questions")
    parser.add_argument("--file", help="Path to text file with questions (one per line)")
    parser.add_argument("--out", default="data/qa_report.json", help="Output JSON report path")
    parser.add_argument("--delay", type=float, default=0.75, help="Delay between questions (seconds)")
    args = parser.parse_args()

    questions = DEFAULT_QUESTIONS
    if args.file:
        questions = load_questions(Path(args.file))

    chat_data = ChatDataService()
    llm_service = LLMService()
    if not llm_service.is_available():
        print("Error: LLM service not available. Configure ANTHROPIC_API_KEY in .env")
        return 1

    results = []
    total = len(questions)
    print(f"Running {total} questions...")

    for idx, q in enumerate(questions, start=1):
        print(f"[{idx}/{total}] {q}")
        context = chat_data.get_context_for_question(q)
        answer = llm_service.answer_question(question=q, context_data=context)
        status = "fail" if is_fail(answer) else "pass"
        results.append({
            "question": q,
            "status": status,
            "answer": answer,
        })
        time.sleep(args.delay)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "total": total,
        "passes": len([r for r in results if r["status"] == "pass"]),
        "fails": len([r for r in results if r["status"] == "fail"]),
        "results": results
    }, indent=2))

    print(f"Report written to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
