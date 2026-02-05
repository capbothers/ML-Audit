"""
Example usage of Product Profitability Intelligence

Shows how to:
1. Calculate product profitability
2. Find losing products
3. Discover hidden gems
4. Get LLM-powered insights
"""
import requests
import json
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"


def analyze_profitability():
    """Calculate profitability for all products"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Analyze Product Profitability")
    print("="*60)

    # Calculate for last 30 days
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=30)

    response = requests.post(
        f"{BASE_URL}/profitability/analyze",
        json={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "period_type": "monthly"
        }
    )

    if response.status_code == 200:
        result = response.json()
        print(f"\n✅ Analysis Complete")
        print(f"Products Analyzed: {result['products_analyzed']}")
        print(f"Period: {result['period']['start'][:10]} to {result['period']['end'][:10]}")

        # Show top 3 most profitable
        if result['results']:
            print("\nTop 3 Most Profitable Products:")
            for i, product in enumerate(result['results'][:3], 1):
                print(f"{i}. {product['title']}")
                print(f"   Net Profit: ${product['net_profit_dollars']:,.2f}")
                print(f"   ROAS: {product.get('roas', 'N/A')}x")
                print(f"   Revenue: ${product['total_revenue']:,.2f}")
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")


def get_profitability_summary():
    """Get overall profitability summary"""
    print("\n" + "="*60)
    print("EXAMPLE 2: Get Profitability Summary")
    print("="*60)

    response = requests.get(f"{BASE_URL}/profitability/summary?days=30")

    if response.status_code == 200:
        summary = response.json()
        print(f"\n📊 PROFITABILITY SUMMARY (Last 30 Days)")
        print(f"\nProducts:")
        print(f"  Total: {summary.get('total_products', 0)}")
        print(f"  Profitable: {summary.get('profitable_products', 0)} ✅")
        print(f"  Losing Money: {summary.get('losing_products', 0)} ❌")
        print(f"  Breakeven: {summary.get('breakeven_products', 0)}")

        print(f"\nFinancials:")
        print(f"  Total Revenue: ${summary.get('total_revenue', 0):,.2f}")
        print(f"  Total Ad Spend: ${summary.get('total_ad_spend', 0):,.2f}")
        print(f"  Total Profit: ${summary.get('total_profit', 0):,.2f}")
        print(f"  Profit Margin: {summary.get('profit_margin_pct', 0):.1f}%")
        print(f"  Blended ROAS: {summary.get('blended_roas', 'N/A')}x")

        if summary.get('top_performer'):
            print(f"\n🏆 Top Performer:")
            print(f"  {summary['top_performer']['title']}")
            print(f"  Profit: ${summary['top_performer']['profit']:,.2f}")

        if summary.get('biggest_loser'):
            print(f"\n⚠️  Biggest Loser:")
            print(f"  {summary['biggest_loser']['title']}")
            print(f"  Loss: ${summary['biggest_loser']['loss']:,.2f}")
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")


def find_losing_products():
    """Find products that are losing money"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Find Products Losing Money")
    print("="*60)

    response = requests.get(f"{BASE_URL}/profitability/losing?days=30&limit=10")

    if response.status_code == 200:
        result = response.json()

        if result['products_found'] > 0:
            print(f"\n⚠️  CRITICAL: {result['products_found']} products are losing money")
            print(f"\n{result['message']}")

            print(f"\nTop 5 Money Losers:")
            for i, product in enumerate(result['products'][:5], 1):
                print(f"\n{i}. {product['title']}")
                print(f"   Revenue: ${product['revenue']:,.2f}")
                print(f"   NET LOSS: ${product['net_profit']:,.2f} ❌")
                print(f"   Ad Spend: ${product['ad_spend']:,.2f}")
                print(f"   ROAS: {product.get('roas', 'N/A')}x")
                print(f"   Return Rate: {product.get('return_rate', 0):.1f}%")
        else:
            print("\n✅ Great news! No products are losing money.")
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")


def find_hidden_gems():
    """Find hidden gems - high ROAS but low revenue products"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Find Hidden Gems")
    print("="*60)

    response = requests.get(
        f"{BASE_URL}/profitability/hidden-gems?days=30&min_roas=4.0&max_revenue=5000"
    )

    if response.status_code == 200:
        result = response.json()

        if result['products_found'] > 0:
            print(f"\n💎 Found {result['products_found']} hidden gems!")
            print(f"\n{result['message']}")
            print(f"\n💡 {result['recommendation']}")

            print(f"\nHidden Gems (High ROAS, Low Revenue):")
            for i, product in enumerate(result['products'][:5], 1):
                print(f"\n{i}. {product['title']}")
                print(f"   ROAS: {product.get('roas', 'N/A')}x ⭐")
                print(f"   Revenue: ${product['revenue']:,.2f}")
                print(f"   Net Profit: ${product['net_profit']:,.2f}")
                print(f"   Ad Spend: ${product['ad_spend']:,.2f}")
                print(f"   → OPPORTUNITY: Increase budget for higher profit")
        else:
            print("\nNo hidden gems found with current criteria.")
            print("Try adjusting min_roas or max_revenue parameters.")
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")


def get_profitability_dashboard():
    """Get complete profitability dashboard"""
    print("\n" + "="*60)
    print("EXAMPLE 5: Profitability Dashboard")
    print("="*60)

    response = requests.get(f"{BASE_URL}/profitability/dashboard?days=30")

    if response.status_code == 200:
        dashboard = response.json()

        print(f"\n📊 COMPLETE PROFITABILITY DASHBOARD")
        print(f"Generated at: {dashboard['generated_at']}")

        summary = dashboard['summary']
        print(f"\n💰 Summary:")
        print(f"  Profit: ${summary.get('total_profit', 0):,.2f}")
        print(f"  Margin: {summary.get('profit_margin_pct', 0):.1f}%")
        print(f"  ROAS: {summary.get('blended_roas', 'N/A')}x")

        print(f"\n🏆 Top Performers ({dashboard['top_performers']['count']}):")
        for product in dashboard['top_performers']['products'][:3]:
            print(f"  • {product['title']}: ${product['net_profit']:,.0f} profit")

        if dashboard['losing_products']['count'] > 0:
            print(f"\n⚠️  Losing Products ({dashboard['losing_products']['count']}):")
            total_loss = dashboard['losing_products']['total_loss']
            print(f"  Total Loss: ${abs(total_loss):,.2f}")
            for product in dashboard['losing_products']['products'][:3]:
                print(f"  • {product['title']}: ${product['net_profit']:,.0f} loss")

        if dashboard['hidden_gems']['count'] > 0:
            print(f"\n💎 Hidden Gems ({dashboard['hidden_gems']['count']}):")
            for product in dashboard['hidden_gems']['products'][:3]:
                print(f"  • {product['title']}: {product.get('roas', 'N/A')}x ROAS")

        print(f"\n🎯 Quick Recommendations:")
        recs = dashboard['recommendations']
        if recs.get('push_harder'):
            print(f"  Push Harder: {', '.join(recs['push_harder'][:3])}")
        if recs.get('reduce_or_fix'):
            print(f"  Fix or Cut: {', '.join(recs['reduce_or_fix'][:3])}")
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")


def get_llm_insights():
    """Get LLM-powered profitability insights"""
    print("\n" + "="*60)
    print("EXAMPLE 6: LLM-Powered Insights (The 'So What?')")
    print("="*60)

    response = requests.get(f"{BASE_URL}/profitability/insights?days=30")

    if response.status_code == 200:
        result = response.json()

        print(f"\n🤖 AI ANALYSIS")
        print(f"\nData Summary:")
        summary = result['data_summary']
        print(f"  Products: {summary['total_products']}")
        print(f"  Profitable: {summary['profitable_count']}")
        print(f"  Losing Money: {summary['losing_count']}")
        print(f"  Total Profit: ${summary['total_profit']:,.2f}")

        print(f"\n📈 Products to Push Harder:")
        for product in result.get('top_performers', [])[:3]:
            print(f"  • {product}")

        if result.get('losing_products'):
            print(f"\n⚠️  Products Losing Money:")
            for product in result['losing_products'][:3]:
                print(f"  • {product}")

        if result.get('hidden_gems'):
            print(f"\n💎 Hidden Gems:")
            for product in result['hidden_gems'][:3]:
                print(f"  • {product}")

        print(f"\n💡 LLM ANALYSIS:")
        print("="*60)
        print(result['llm_analysis'])
        print("="*60)
    else:
        if response.status_code == 503:
            print("\n⚠️  LLM service not available")
            print("Configure ANTHROPIC_API_KEY in .env to enable AI insights")
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")


def analyze_specific_product():
    """Deep-dive analysis for a specific product"""
    print("\n" + "="*60)
    print("EXAMPLE 7: Product-Specific Deep Dive")
    print("="*60)

    # First get product list to find an ID
    response = requests.get(f"{BASE_URL}/profitability/losing?days=30&limit=1")

    if response.status_code == 200 and response.json()['products_found'] > 0:
        product = response.json()['products'][0]
        product_id = product['product_id']

        print(f"\nAnalyzing: {product['title']}")

        # Get deep-dive analysis
        response = requests.get(
            f"{BASE_URL}/profitability/insights/product/{product_id}?days=30"
        )

        if response.status_code == 200:
            result = response.json()

            print(f"\n💰 Financial Summary:")
            fs = result['financial_summary']
            print(f"  Revenue: ${fs['revenue']:,.2f}")
            print(f"  COGS: ${fs['cogs']:,.2f}")
            print(f"  Ad Spend: ${fs['ad_spend']:,.2f}")
            print(f"  Refunds: ${fs['refunds']:,.2f}")
            print(f"  NET PROFIT: ${fs['net_profit']:,.2f}")
            print(f"  ROAS: {fs.get('roas', 'N/A')}x")
            print(f"  Return Rate: {fs['return_rate']:.1f}%")

            if result.get('llm_explanation'):
                print(f"\n🤖 LLM DIAGNOSIS:")
                print("="*60)
                print(result['llm_explanation'])
                print("="*60)
        else:
            if response.status_code == 503:
                print("\n⚠️  LLM service not available")
            else:
                print(f"❌ Error: {response.status_code}")
    else:
        print("\nNo losing products found to analyze")
        print("Try running with a profitable product ID instead")


def main():
    """Run all examples"""
    print("\n" + "="*60)
    print("PRODUCT PROFITABILITY INTELLIGENCE EXAMPLES")
    print("="*60)
    print("\nThese examples show how to find:")
    print("  • Which products are actually making money")
    print("  • Which 'best sellers' are losing money")
    print("  • Hidden gems that need more budget")
    print("  • Where to spend your ad budget")

    try:
        # Run examples
        analyze_profitability()
        get_profitability_summary()
        find_losing_products()
        find_hidden_gems()
        get_profitability_dashboard()
        get_llm_insights()
        analyze_specific_product()

        print("\n" + "="*60)
        print("✅ All examples completed!")
        print("="*60)
        print("\nNext Steps:")
        print("1. Review the dashboard: GET /profitability/dashboard")
        print("2. Get LLM insights: GET /profitability/insights")
        print("3. Act on losing products immediately")
        print("4. Scale up hidden gems gradually")

    except requests.exceptions.ConnectionError:
        print("\n❌ Error: Cannot connect to ML-Audit server")
        print("Make sure the server is running:")
        print("  python app/main.py")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")


if __name__ == "__main__":
    main()
