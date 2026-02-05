"""
Analysis Service
Runs ML analysis and generates insights
"""
from typing import Dict, List, Optional
from datetime import datetime

from app.ml.churn_prediction import ChurnPredictor
from app.ml.anomaly_detection import AnomalyDetector
from app.ml.seo_analyzer import SEOAnalyzer
from app.ml.recommendation_engine import RecommendationEngine
from app.services.llm_service import LLMService
from app.utils.logger import log


class AnalysisService:
    """
    Orchestrates all ML analysis and insight generation
    """

    def __init__(self):
        self.churn_predictor = ChurnPredictor()
        self.anomaly_detector = AnomalyDetector()
        self.seo_analyzer = SEOAnalyzer()
        self.recommendation_engine = RecommendationEngine()
        self.llm_service = LLMService()

    async def run_full_analysis(
        self,
        customer_data: Optional[List[Dict]] = None,
        campaign_data: Optional[List[Dict]] = None,
        traffic_data: Optional[List[Dict]] = None,
        revenue_data: Optional[List[Dict]] = None,
        disapproved_ads: Optional[List[Dict]] = None,
        abandoned_checkouts: Optional[List[Dict]] = None,
        email_campaigns: Optional[List[Dict]] = None,
        urls_to_audit: Optional[List[str]] = None
    ) -> Dict:
        """
        Run complete analysis across all modules
        """
        log.info("Starting full analysis...")

        results = {
            'timestamp': datetime.utcnow().isoformat(),
            'churn_analysis': None,
            'anomalies': [],
            'seo_audit': None,
            'recommendations': [],
            'insights': []
        }

        # Churn Prediction
        if customer_data:
            log.info("Running churn prediction...")
            try:
                churn_results = self.churn_predictor.predict(customer_data)
                high_risk = [c for c in churn_results if c['churn_risk_level'] == 'HIGH']

                results['churn_analysis'] = {
                    'total_customers_analyzed': len(churn_results),
                    'high_risk_count': len(high_risk),
                    'medium_risk_count': len([c for c in churn_results if c['churn_risk_level'] == 'MEDIUM']),
                    'low_risk_count': len([c for c in churn_results if c['churn_risk_level'] == 'LOW']),
                    'high_risk_customers': high_risk[:20],  # Top 20
                    'total_value_at_risk': sum(c.get('total_spent', 0) for c in high_risk)
                }
            except Exception as e:
                log.error(f"Churn prediction error: {str(e)}")
                results['churn_analysis'] = {'error': str(e)}

        # Anomaly Detection
        anomalies = []

        if campaign_data:
            log.info("Detecting campaign anomalies...")
            try:
                campaign_anomalies = self.anomaly_detector.detect_campaign_anomalies(campaign_data)
                anomalies.extend(campaign_anomalies)
            except Exception as e:
                log.error(f"Campaign anomaly detection error: {str(e)}")

        if traffic_data:
            log.info("Detecting traffic anomalies...")
            try:
                traffic_anomalies = self.anomaly_detector.detect_traffic_anomalies(traffic_data)
                anomalies.extend(traffic_anomalies)
            except Exception as e:
                log.error(f"Traffic anomaly detection error: {str(e)}")

        if revenue_data:
            log.info("Detecting revenue anomalies...")
            try:
                revenue_anomalies = self.anomaly_detector.detect_revenue_anomalies(revenue_data)
                anomalies.extend(revenue_anomalies)
            except Exception as e:
                log.error(f"Revenue anomaly detection error: {str(e)}")

        results['anomalies'] = anomalies

        # SEO Audit
        if urls_to_audit:
            log.info(f"Running SEO audit on {len(urls_to_audit)} URLs...")
            try:
                results['seo_audit'] = self.seo_analyzer.audit_site(urls_to_audit)
            except Exception as e:
                log.error(f"SEO audit error: {str(e)}")
                results['seo_audit'] = {'error': str(e)}

        # Generate Recommendations
        log.info("Generating recommendations...")
        try:
            churn_data_for_recs = results['churn_analysis'].get('high_risk_customers', []) if results['churn_analysis'] else None

            seo_issues = None
            if results['seo_audit'] and 'pages' in results['seo_audit']:
                seo_issues = []
                for page in results['seo_audit']['pages']:
                    seo_issues.extend(page.get('issues', []))

            recommendations = self.recommendation_engine.generate_recommendations(
                churn_data=churn_data_for_recs,
                anomalies=anomalies,
                seo_issues=seo_issues,
                campaign_data=campaign_data,
                disapproved_ads=disapproved_ads,
                abandoned_checkouts=abandoned_checkouts,
                email_campaigns=email_campaigns
            )

            results['recommendations'] = recommendations
            results['executive_summary'] = self.recommendation_engine.generate_executive_summary(recommendations)

            # Generate LLM-powered insights
            if self.llm_service.is_available():
                log.info("Generating LLM-powered insights...")

                # Executive summary with LLM
                results['llm_executive_summary'] = self.llm_service.generate_executive_summary(results)

                # Churn explanation
                if churn_data_for_recs:
                    results['llm_churn_explanation'] = self.llm_service.explain_churn_predictions(churn_data_for_recs)

                # Recommendations explanation
                results['llm_recommendations'] = self.llm_service.explain_recommendations(recommendations)

                log.info("LLM insights generated")
            else:
                log.info("LLM service not available, skipping AI-powered insights")

        except Exception as e:
            log.error(f"Recommendation generation error: {str(e)}")
            results['recommendations'] = []

        log.info("Full analysis complete")
        return results

    async def predict_churn(self, customer_data: List[Dict]) -> List[Dict]:
        """Run churn prediction only"""
        return self.churn_predictor.predict(customer_data)

    async def train_churn_model(self, customer_data: List[Dict]) -> Dict:
        """Train churn prediction model"""
        return self.churn_predictor.train(customer_data)

    async def detect_anomalies(
        self,
        data: List[Dict],
        metric_name: str,
        method: str = 'zscore'
    ) -> List[Dict]:
        """Detect anomalies in a specific metric"""
        return self.anomaly_detector.detect_metric_anomalies(
            data,
            metric_name,
            method=method
        )

    async def audit_seo(self, urls: List[str]) -> Dict:
        """Run SEO audit"""
        return self.seo_analyzer.audit_site(urls)

    async def get_recommendations(
        self,
        data_sources: Dict
    ) -> List[Dict]:
        """Generate recommendations from various data sources"""
        return self.recommendation_engine.generate_recommendations(**data_sources)
