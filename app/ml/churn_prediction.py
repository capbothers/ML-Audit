"""
Customer Churn Prediction Module
Predicts which customers are likely to churn using ML
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib
from pathlib import Path

from app.utils.logger import log
from app.config import get_settings

settings = get_settings()


class ChurnPredictor:
    """
    Predicts customer churn probability using behavioral features
    """

    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = []
        self.model_path = Path(settings.ml_model_path) / "churn_model.pkl"
        self.scaler_path = Path(settings.ml_model_path) / "churn_scaler.pkl"

    def prepare_features(self, customer_data: List[Dict]) -> pd.DataFrame:
        """
        Engineer features from customer data for churn prediction
        """
        df = pd.DataFrame(customer_data)

        # Convert dates
        df['last_order_date'] = pd.to_datetime(df.get('last_order_date'))
        df['created_at'] = pd.to_datetime(df.get('created_at'))

        # Calculate recency, frequency, monetary features
        current_date = datetime.utcnow()

        # Recency (days since last order)
        df['days_since_last_order'] = (current_date - df['last_order_date']).dt.days
        df['days_since_last_order'] = df['days_since_last_order'].fillna(999)

        # Frequency
        df['orders_count'] = df.get('orders_count', 0)

        # Monetary
        df['total_spent'] = df.get('total_spent', 0.0)
        df['average_order_value'] = df.get('average_order_value', 0.0)

        # Customer lifetime (days since first order)
        df['customer_lifetime_days'] = (current_date - df['created_at']).dt.days

        # Engagement features
        df['accepts_marketing'] = df.get('accepts_marketing', False).astype(int)
        df['klaviyo_engaged'] = df.get('klaviyo_engaged', False).astype(int)

        # Email engagement recency
        df['last_email_open_date'] = pd.to_datetime(df.get('last_email_open_date'))
        df['days_since_email_open'] = (current_date - df['last_email_open_date']).dt.days
        df['days_since_email_open'] = df['days_since_email_open'].fillna(999)

        # Calculated features
        df['purchase_frequency'] = df['orders_count'] / (df['customer_lifetime_days'] + 1)
        df['engagement_score'] = (
            df['accepts_marketing'] * 0.3 +
            df['klaviyo_engaged'] * 0.4 +
            (df['days_since_email_open'] < 30).astype(int) * 0.3
        )

        # Select features for model
        feature_cols = [
            'days_since_last_order',
            'orders_count',
            'total_spent',
            'average_order_value',
            'customer_lifetime_days',
            'accepts_marketing',
            'klaviyo_engaged',
            'days_since_email_open',
            'purchase_frequency',
            'engagement_score'
        ]

        self.feature_names = feature_cols
        return df[feature_cols].fillna(0)

    def create_training_labels(self, customer_data: List[Dict]) -> np.ndarray:
        """
        Create training labels (churned = 1, active = 0)
        Define churn as: no purchase in last 90 days AND had previous purchases
        """
        df = pd.DataFrame(customer_data)
        df['last_order_date'] = pd.to_datetime(df.get('last_order_date'))

        current_date = datetime.utcnow()
        churn_threshold_days = 90

        df['days_since_last_order'] = (current_date - df['last_order_date']).dt.days
        df['orders_count'] = df.get('orders_count', 0)

        # Customer has churned if they haven't ordered in 90+ days but had orders before
        churned = (
            (df['days_since_last_order'] > churn_threshold_days) &
            (df['orders_count'] > 0)
        ).astype(int)

        return churned.values

    def train(self, customer_data: List[Dict]) -> Dict:
        """
        Train churn prediction model
        """
        log.info("Training churn prediction model...")

        # Prepare features and labels
        X = self.prepare_features(customer_data)
        y = self.create_training_labels(customer_data)

        # Check if we have enough data
        if len(X) < 50:
            log.warning("Insufficient data for training (need at least 50 customers)")
            return {"success": False, "error": "Insufficient data"}

        # Check class balance
        churn_rate = y.sum() / len(y)
        log.info(f"Churn rate in training data: {churn_rate:.2%}")

        if churn_rate == 0 or churn_rate == 1:
            log.warning("Imbalanced data: all customers are in one class")
            return {"success": False, "error": "Imbalanced data"}

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Train model (using Gradient Boosting for better performance)
        self.model = GradientBoostingClassifier(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            random_state=42
        )
        self.model.fit(X_train_scaled, y_train)

        # Evaluate
        train_score = self.model.score(X_train_scaled, y_train)
        test_score = self.model.score(X_test_scaled, y_test)

        # Feature importance
        feature_importance = dict(zip(
            self.feature_names,
            self.model.feature_importances_
        ))

        log.info(f"Model trained - Train accuracy: {train_score:.3f}, Test accuracy: {test_score:.3f}")

        # Save model
        self.save_model()

        return {
            "success": True,
            "train_accuracy": train_score,
            "test_accuracy": test_score,
            "churn_rate": churn_rate,
            "feature_importance": feature_importance,
            "samples_trained": len(X_train)
        }

    def predict(self, customer_data: List[Dict]) -> List[Dict]:
        """
        Predict churn probability for customers
        """
        if self.model is None:
            self.load_model()

        if self.model is None:
            log.error("No trained model available")
            return []

        # Prepare features
        X = self.prepare_features(customer_data)
        X_scaled = self.scaler.transform(X)

        # Predict probabilities
        churn_probabilities = self.model.predict_proba(X_scaled)[:, 1]

        # Create results
        results = []
        for i, prob in enumerate(churn_probabilities):
            # Determine risk level
            if prob >= 0.7:
                risk_level = "HIGH"
            elif prob >= 0.4:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"

            results.append({
                "customer_id": customer_data[i].get('id'),
                "email": customer_data[i].get('email'),
                "churn_probability": float(prob),
                "churn_risk_level": risk_level,
                "days_since_last_order": customer_data[i].get('days_since_last_order'),
                "total_spent": customer_data[i].get('total_spent', 0),
                "orders_count": customer_data[i].get('orders_count', 0)
            })

        log.info(f"Predicted churn for {len(results)} customers")
        return results

    def get_high_risk_customers(self, customer_data: List[Dict], threshold: float = 0.7) -> List[Dict]:
        """
        Get customers at high risk of churning
        """
        predictions = self.predict(customer_data)
        high_risk = [p for p in predictions if p['churn_probability'] >= threshold]

        # Sort by churn probability
        high_risk.sort(key=lambda x: x['churn_probability'], reverse=True)

        log.info(f"Found {len(high_risk)} high-risk customers")
        return high_risk

    def calculate_predicted_ltv(self, customer_data: Dict) -> float:
        """
        Calculate predicted customer lifetime value
        Simple model: AOV * predicted future orders
        """
        avg_order_value = customer_data.get('average_order_value', 0)
        orders_count = customer_data.get('orders_count', 0)
        lifetime_days = customer_data.get('customer_lifetime_days', 1)

        if lifetime_days == 0 or orders_count == 0:
            return avg_order_value

        # Calculate purchase frequency (orders per year)
        orders_per_year = (orders_count / lifetime_days) * 365

        # Predict 2 years of future value
        predicted_future_orders = orders_per_year * 2
        predicted_ltv = avg_order_value * predicted_future_orders

        return predicted_ltv

    def save_model(self):
        """Save trained model to disk"""
        Path(settings.ml_model_path).mkdir(parents=True, exist_ok=True)
        joblib.dump(self.model, self.model_path)
        joblib.dump(self.scaler, self.scaler_path)
        log.info(f"Model saved to {self.model_path}")

    def load_model(self):
        """Load trained model from disk"""
        try:
            if self.model_path.exists():
                self.model = joblib.load(self.model_path)
                self.scaler = joblib.load(self.scaler_path)
                log.info(f"Model loaded from {self.model_path}")
            else:
                log.warning("No saved model found")
        except Exception as e:
            log.error(f"Error loading model: {str(e)}")
