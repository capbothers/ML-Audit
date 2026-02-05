# ML-Audit Growth Intelligence Platform

An AI-powered growth intelligence platform that helps e-commerce businesses optimize their marketing, reduce churn, and identify hidden opportunities that humans can't easily spot.

## 🎯 What It Does

This platform acts as an AI-powered assistant for Heads of Growth and Digital Managers, providing:

### **Customer Intelligence**
- **Churn Prediction**: Identifies customers at risk of churning before they leave
- **Lifetime Value Prediction**: Estimates future customer value
- **Behavioral Segmentation**: Groups customers by engagement patterns

### **Anomaly Detection**
- **Traffic Anomalies**: Detects unusual drops or spikes in website traffic
- **Revenue Anomalies**: Identifies unexpected revenue changes
- **Campaign Anomalies**: Spots unusual ad performance patterns
- **Conversion Rate Changes**: Finds statistically significant conversion shifts

### **Marketing Optimization**
- **Google Ads Monitoring**: Tracks campaign performance and detects disapproved ads
- **Email Campaign Analysis**: Evaluates Klaviyo campaign performance
- **ROI Optimization**: Identifies underperforming campaigns

### **SEO & Technical**
- **Technical SEO Audits**: Finds technical issues hurting rankings
- **Content Analysis**: Evaluates page optimization
- **Performance Monitoring**: Tracks site speed and mobile-friendliness

### **Product Profitability Intelligence** ⭐ NEW
- **True Profitability**: Calculate real product profit after COGS, ad spend, and returns
- **Hidden Gems**: Find high-ROAS products that need more budget
- **Money Losers**: Identify "best sellers" that are actually losing money
- **Budget Optimization**: AI-powered recommendations on where to spend ad budget
- **LLM Analysis**: Get specific answers to "which products should I push?"

### **Actionable Recommendations**
- Prioritized action items with estimated impact
- Automated alerts for critical issues
- Executive summaries for stakeholders

## 🔌 Integrations

- **Shopify** - E-commerce data (orders, customers, products, abandoned carts)
- **Klaviyo** - Email marketing (campaigns, flows, engagement)
- **Google Analytics 4** - Website analytics (traffic, conversions, behavior)
- **Google Ads** - Paid advertising (campaigns, ads, keywords, performance)

## 🚀 Quick Start

See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed setup instructions.

```bash
# Install
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your API credentials

# Run
python app/main.py
```

Access the API at http://localhost:8000/docs

## 📊 Key Features

### Churn Prediction
Identifies customers likely to churn using ML models trained on purchase behavior, engagement, and RFM analysis.

### Anomaly Detection
Multiple detection algorithms (Z-score, IQR, Isolation Forest) to find unusual patterns in metrics that humans miss.

### SEO Analysis
Comprehensive technical audits covering 30+ SEO factors.

### Smart Recommendations
AI-generated, prioritized action items with estimated impact.

## 🏗️ Architecture

Built with:
- **FastAPI** - High-performance API framework
- **PostgreSQL** - Data storage
- **scikit-learn** - Machine learning
- **pandas/numpy** - Data processing
- **Redis** - Caching (optional)

## 📝 API Examples

```python
# Predict churn
POST /insights/churn/predict
{
  "customer_data": [...]
}

# Detect anomalies
POST /insights/anomalies/detect
{
  "data": [...],
  "metric_name": "revenue",
  "method": "zscore"
}

# Run full analysis
POST /insights/analyze
{
  "customer_data": [...],
  "campaign_data": [...],
  "traffic_data": [...]
}

# Product profitability analysis
POST /profitability/analyze
{
  "start_date": "2026-01-01",
  "end_date": "2026-01-31"
}

# Get LLM-powered profitability insights
GET /profitability/insights?days=30
# Returns AI analysis of which products to push, cut, or fix
```

## 🚨 Automated Alerts

Configure email and Slack alerts for:
- High-risk churn customers
- Traffic/revenue anomalies
- Disapproved Google Ads
- Critical SEO issues

## 📈 Production Deployment

```bash
# Using Docker
docker-compose up -d

# Or deploy to cloud
# Supports: AWS, GCP, Azure, Heroku
```

## 🤝 Contributing

Contributions welcome! See issues for ideas.

## 📄 License

MIT License

---

Built to help businesses grow smarter with AI-powered insights.