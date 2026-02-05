"""
Example usage of LLM-powered insights
"""
import requests
import json

BASE_URL = "http://localhost:8000"


def check_llm_status():
    """Check if LLM service is available"""
    response = requests.get(f"{BASE_URL}/llm/status")
    print("LLM Status:", response.json())
    return response.json()['available']


def ask_question_example():
    """Example: Ask a natural language question"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Ask a Question")
    print("="*60)

    question = "Why did my revenue drop last week?"

    # Sample context data (replace with real data)
    context_data = {
        "revenue_last_week": 5000,
        "revenue_previous_week": 8500,
        "traffic_last_week": 1200,
        "traffic_previous_week": 2000,
        "campaigns": [
            {"name": "Winter Sale", "status": "paused", "date_paused": "2026-01-15"},
            {"name": "Retargeting", "status": "active", "spend": 500}
        ]
    }

    response = requests.post(
        f"{BASE_URL}/llm/ask",
        json={
            "question": question,
            "context_data": context_data
        }
    )

    if response.status_code == 200:
        result = response.json()
        print(f"\nQuestion: {result['question']}")
        print(f"\nAnswer:\n{result['answer']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")


def explain_anomaly_example():
    """Example: Explain an anomaly"""
    print("\n" + "="*60)
    print("EXAMPLE 2: Explain Anomaly")
    print("="*60)

    anomaly = {
        "metric": "traffic",
        "value": 1200,
        "expected_value": 2000,
        "deviation_pct": -40.0,
        "direction": "drop",
        "severity": "critical",
        "date": "2026-01-15",
        "type": "traffic_anomaly"
    }

    response = requests.post(
        f"{BASE_URL}/llm/explain/anomaly",
        json={"anomaly": anomaly}
    )

    if response.status_code == 200:
        result = response.json()
        print(f"\nAnomaly: {anomaly['metric']} dropped {abs(anomaly['deviation_pct'])}%")
        print(f"\nExplanation:\n{result['explanation']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")


def explain_recommendations_example():
    """Example: Explain recommendations"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Explain Recommendations")
    print("="*60)

    recommendations = [
        {
            "priority": "critical",
            "title": "47 High-Risk Customers Need Immediate Attention",
            "description": "47 customers at high risk of churning",
            "impact": 23450,
            "type": "churn_prevention"
        },
        {
            "priority": "high",
            "title": "3 Google Ads Disapproved",
            "description": "3 ads are not running due to policy violations",
            "impact": 1500,
            "type": "ad_disapproval"
        },
        {
            "priority": "high",
            "title": "Abandoned Checkouts Worth $8,200",
            "description": "Significant revenue in abandoned carts",
            "impact": 2460,
            "type": "abandoned_checkout"
        }
    ]

    response = requests.post(
        f"{BASE_URL}/llm/explain/recommendations",
        json={"recommendations": recommendations}
    )

    if response.status_code == 200:
        result = response.json()
        print(f"\nTotal Recommendations: {result['total_recommendations']}")
        print(f"\nAction Plan:\n{result['explanation']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")


def generate_winback_email_example():
    """Example: Generate a win-back email"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Generate Win-Back Email")
    print("="*60)

    customer = {
        "email": "sarah@example.com",
        "first_name": "Sarah",
        "total_spent": 850.00,
        "orders_count": 5,
        "days_since_last_order": 75,
        "churn_probability": 0.85
    }

    response = requests.post(
        f"{BASE_URL}/llm/generate/email/winback",
        json={"customer": customer}
    )

    if response.status_code == 200:
        result = response.json()
        print(f"\nFor: {result['customer_email']}")
        print(f"\nGenerated Email:\n{result['generated_email']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")


def full_analysis_with_llm():
    """Example: Run full analysis with LLM insights"""
    print("\n" + "="*60)
    print("EXAMPLE 5: Full Analysis with LLM")
    print("="*60)

    # Sample data (replace with real data from your connectors)
    analysis_request = {
        "customer_data": [
            {
                "id": 1,
                "email": "customer@example.com",
                "orders_count": 5,
                "total_spent": 500.0,
                "average_order_value": 100.0,
                "last_order_date": "2025-11-01T00:00:00",
                "created_at": "2024-01-01T00:00:00",
                "accepts_marketing": True,
                "klaviyo_engaged": True,
                "customer_lifetime_days": 365
            }
        ],
        "traffic_data": [
            {"date": "2026-01-15", "sessions": 1200, "active_users": 800, "pageviews": 3500},
            {"date": "2026-01-16", "sessions": 2000, "active_users": 1400, "pageviews": 6000},
        ],
        "revenue_data": [
            {"date": "2026-01-15", "revenue": 5000},
            {"date": "2026-01-16", "revenue": 8500},
        ]
    }

    response = requests.post(
        f"{BASE_URL}/insights/analyze",
        json=analysis_request
    )

    if response.status_code == 200:
        result = response.json()

        print("\n--- Standard ML Insights ---")
        if result.get('churn_analysis'):
            print(f"High-risk customers: {result['churn_analysis'].get('high_risk_count', 0)}")

        print(f"Anomalies detected: {len(result.get('anomalies', []))}")
        print(f"Recommendations: {len(result.get('recommendations', []))}")

        print("\n--- LLM-Powered Insights ---")
        if result.get('llm_executive_summary'):
            print("\nExecutive Summary:")
            print(result['llm_executive_summary'])

        if result.get('llm_churn_explanation'):
            print("\nChurn Analysis:")
            print(result['llm_churn_explanation'])

        if result.get('llm_recommendations'):
            print("\nRecommendations Explained:")
            print(result['llm_recommendations'])
    else:
        print(f"Error: {response.status_code} - {response.text}")


def main():
    """Run all examples"""
    print("\n" + "="*60)
    print("ML-Audit LLM Examples")
    print("="*60)

    # Check if LLM is available
    if not check_llm_status():
        print("\n⚠️  LLM service not available!")
        print("Configure ANTHROPIC_API_KEY in .env to enable LLM features")
        return

    print("\n✅ LLM service is available!")

    # Run examples
    try:
        ask_question_example()
        explain_anomaly_example()
        explain_recommendations_example()
        generate_winback_email_example()
        full_analysis_with_llm()

        print("\n" + "="*60)
        print("All examples completed!")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Error running examples: {str(e)}")
        print("Make sure the ML-Audit server is running:")
        print("  python app/main.py")


if __name__ == "__main__":
    main()
