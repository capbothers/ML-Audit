"""
Anomaly Detection Module
Detects unusual patterns in campaigns, traffic, revenue, and other metrics
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from scipy import stats
from pyod.models.iforest import IForest
from pyod.models.knn import KNN

from app.utils.logger import log
from app.config import get_settings

settings = get_settings()


class AnomalyDetector:
    """
    Detects anomalies across various metrics using multiple techniques
    """

    def __init__(self, sensitivity: float = None):
        """
        Initialize anomaly detector

        Args:
            sensitivity: Detection sensitivity (0-1), lower = more sensitive
        """
        self.sensitivity = sensitivity or settings.anomaly_detection_sensitivity
        self.models = {}

    def detect_metric_anomalies(
        self,
        data: List[Dict],
        metric_name: str,
        date_column: str = 'date',
        method: str = 'zscore'
    ) -> List[Dict]:
        """
        Detect anomalies in a time series metric

        Args:
            data: List of data points with date and metric
            metric_name: Name of the metric to analyze
            date_column: Name of the date column
            method: 'zscore', 'iqr', or 'isolation_forest'

        Returns:
            List of anomalies with details
        """
        if not data:
            return []

        df = pd.DataFrame(data)

        if metric_name not in df.columns:
            log.warning(f"Metric {metric_name} not found in data")
            return []

        # Sort by date
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.sort_values(date_column)

        # Detect anomalies based on method
        if method == 'zscore':
            anomalies = self._detect_zscore_anomalies(df, metric_name, date_column)
        elif method == 'iqr':
            anomalies = self._detect_iqr_anomalies(df, metric_name, date_column)
        elif method == 'isolation_forest':
            anomalies = self._detect_isolation_forest_anomalies(df, metric_name, date_column)
        else:
            log.error(f"Unknown anomaly detection method: {method}")
            return []

        return anomalies

    def _detect_zscore_anomalies(
        self,
        df: pd.DataFrame,
        metric_name: str,
        date_column: str
    ) -> List[Dict]:
        """
        Detect anomalies using Z-score method (statistical outliers)
        """
        values = df[metric_name].values
        mean = np.mean(values)
        std = np.std(values)

        if std == 0:
            return []

        # Calculate Z-scores
        z_scores = np.abs((values - mean) / std)

        # Threshold based on sensitivity (default: 3 standard deviations)
        threshold = 3 - (self.sensitivity * 20)  # More sensitive = lower threshold

        # Find anomalies
        anomaly_indices = np.where(z_scores > threshold)[0]

        anomalies = []
        for idx in anomaly_indices:
            row = df.iloc[idx]
            anomalies.append({
                'date': row[date_column],
                'metric': metric_name,
                'value': float(row[metric_name]),
                'expected_value': float(mean),
                'z_score': float(z_scores[idx]),
                'deviation_pct': float(((row[metric_name] - mean) / mean) * 100) if mean != 0 else 0,
                'direction': 'spike' if row[metric_name] > mean else 'drop',
                'severity': self._calculate_severity(z_scores[idx])
            })

        log.info(f"Found {len(anomalies)} Z-score anomalies in {metric_name}")
        return anomalies

    def _detect_iqr_anomalies(
        self,
        df: pd.DataFrame,
        metric_name: str,
        date_column: str
    ) -> List[Dict]:
        """
        Detect anomalies using Interquartile Range (IQR) method
        """
        values = df[metric_name].values
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1

        if iqr == 0:
            return []

        # IQR bounds (adjusted by sensitivity)
        multiplier = 1.5 + (1 - self.sensitivity * 10)
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        # Find anomalies
        anomaly_mask = (values < lower_bound) | (values > upper_bound)
        anomaly_indices = np.where(anomaly_mask)[0]

        median = np.median(values)

        anomalies = []
        for idx in anomaly_indices:
            row = df.iloc[idx]
            anomalies.append({
                'date': row[date_column],
                'metric': metric_name,
                'value': float(row[metric_name]),
                'expected_value': float(median),
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound),
                'deviation_pct': float(((row[metric_name] - median) / median) * 100) if median != 0 else 0,
                'direction': 'spike' if row[metric_name] > median else 'drop',
                'severity': self._calculate_iqr_severity(row[metric_name], lower_bound, upper_bound, median)
            })

        log.info(f"Found {len(anomalies)} IQR anomalies in {metric_name}")
        return anomalies

    def _detect_isolation_forest_anomalies(
        self,
        df: pd.DataFrame,
        metric_name: str,
        date_column: str
    ) -> List[Dict]:
        """
        Detect anomalies using Isolation Forest (ML-based)
        """
        values = df[metric_name].values.reshape(-1, 1)

        if len(values) < 10:
            log.warning("Insufficient data for Isolation Forest (need at least 10 points)")
            return []

        # Train Isolation Forest
        clf = IForest(contamination=self.sensitivity)
        clf.fit(values)

        # Predict anomalies (-1 = anomaly, 1 = normal)
        predictions = clf.predict(values)
        anomaly_scores = clf.decision_scores_

        # Find anomalies
        anomaly_indices = np.where(predictions == 1)[0]  # PyOD uses 1 for outliers

        median = np.median(values)

        anomalies = []
        for idx in anomaly_indices:
            row = df.iloc[idx]
            anomalies.append({
                'date': row[date_column],
                'metric': metric_name,
                'value': float(row[metric_name]),
                'expected_value': float(median),
                'anomaly_score': float(anomaly_scores[idx]),
                'deviation_pct': float(((row[metric_name] - median) / median) * 100) if median != 0 else 0,
                'direction': 'spike' if row[metric_name] > median else 'drop',
                'severity': self._calculate_ml_severity(anomaly_scores[idx])
            })

        log.info(f"Found {len(anomalies)} Isolation Forest anomalies in {metric_name}")
        return anomalies

    def detect_campaign_anomalies(self, campaign_data: List[Dict]) -> List[Dict]:
        """
        Detect anomalies in campaign performance metrics
        """
        if not campaign_data:
            return []

        df = pd.DataFrame(campaign_data)

        # Metrics to check
        metrics_to_check = ['cost', 'ctr', 'conversions', 'cpa']

        all_anomalies = []

        for metric in metrics_to_check:
            if metric not in df.columns:
                continue

            # Filter out zero/null values for some metrics
            if metric in ['ctr', 'cpa']:
                df_filtered = df[df[metric] > 0]
            else:
                df_filtered = df

            if len(df_filtered) < 5:
                continue

            # Detect anomalies
            anomalies = self._detect_zscore_anomalies(df_filtered, metric, 'date')

            # Add campaign context
            for anomaly in anomalies:
                campaign = df_filtered[df_filtered['date'] == anomaly['date']].iloc[0]
                anomaly['campaign_id'] = campaign.get('id')
                anomaly['campaign_name'] = campaign.get('name')
                anomaly['type'] = 'campaign_anomaly'

                all_anomalies.append(anomaly)

        log.info(f"Found {len(all_anomalies)} campaign anomalies")
        return all_anomalies

    def detect_revenue_anomalies(self, revenue_data: List[Dict]) -> List[Dict]:
        """
        Detect unusual revenue patterns
        """
        anomalies = self.detect_metric_anomalies(
            revenue_data,
            metric_name='revenue',
            date_column='date',
            method='zscore'
        )

        # Classify anomalies
        for anomaly in anomalies:
            anomaly['type'] = 'revenue_anomaly'

            # Add business context
            if anomaly['direction'] == 'drop':
                anomaly['impact'] = 'negative'
                anomaly['priority'] = 'high' if abs(anomaly['deviation_pct']) > 30 else 'medium'
            else:
                anomaly['impact'] = 'positive'
                anomaly['priority'] = 'low'

        return anomalies

    def detect_traffic_anomalies(self, traffic_data: List[Dict]) -> List[Dict]:
        """
        Detect unusual traffic patterns (sessions, users, etc.)
        """
        metrics = ['sessions', 'active_users', 'pageviews']

        all_anomalies = []

        for metric in metrics:
            anomalies = self.detect_metric_anomalies(
                traffic_data,
                metric_name=metric,
                date_column='date',
                method='zscore'
            )

            for anomaly in anomalies:
                anomaly['type'] = 'traffic_anomaly'
                anomaly['traffic_metric'] = metric

                # Traffic drops are usually concerning
                if anomaly['direction'] == 'drop':
                    anomaly['priority'] = 'high'
                else:
                    anomaly['priority'] = 'medium'

                all_anomalies.append(anomaly)

        log.info(f"Found {len(all_anomalies)} traffic anomalies")
        return all_anomalies

    def detect_conversion_rate_changes(
        self,
        conversion_data: List[Dict],
        significance_level: float = 0.05
    ) -> List[Dict]:
        """
        Detect statistically significant changes in conversion rates
        """
        if len(conversion_data) < 14:  # Need at least 2 weeks of data
            return []

        df = pd.DataFrame(conversion_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # Calculate conversion rate
        if 'conversion_rate' not in df.columns:
            df['conversion_rate'] = (
                df['conversions'] / df['sessions']
            ).fillna(0) * 100

        # Compare recent week to previous weeks
        recent_week = df.tail(7)
        previous_weeks = df.iloc[-21:-7]  # 2 weeks before

        if len(previous_weeks) == 0:
            return []

        recent_cr = recent_week['conversion_rate'].mean()
        previous_cr = previous_weeks['conversion_rate'].mean()

        # Statistical test
        t_stat, p_value = stats.ttest_ind(
            recent_week['conversion_rate'],
            previous_weeks['conversion_rate']
        )

        changes = []

        if p_value < significance_level:
            pct_change = ((recent_cr - previous_cr) / previous_cr) * 100 if previous_cr != 0 else 0

            changes.append({
                'type': 'conversion_rate_change',
                'metric': 'conversion_rate',
                'recent_value': float(recent_cr),
                'previous_value': float(previous_cr),
                'change_pct': float(pct_change),
                'p_value': float(p_value),
                'direction': 'increase' if recent_cr > previous_cr else 'decrease',
                'statistically_significant': True,
                'priority': 'high' if abs(pct_change) > 20 else 'medium'
            })

        return changes

    def _calculate_severity(self, z_score: float) -> str:
        """Calculate severity based on Z-score"""
        if abs(z_score) > 5:
            return "critical"
        elif abs(z_score) > 4:
            return "high"
        elif abs(z_score) > 3:
            return "medium"
        else:
            return "low"

    def _calculate_iqr_severity(
        self,
        value: float,
        lower_bound: float,
        upper_bound: float,
        median: float
    ) -> str:
        """Calculate severity based on IQR distance"""
        if value < lower_bound:
            distance = (lower_bound - value) / (median - lower_bound) if median != lower_bound else 0
        else:
            distance = (value - upper_bound) / (upper_bound - median) if upper_bound != median else 0

        if distance > 3:
            return "critical"
        elif distance > 2:
            return "high"
        elif distance > 1:
            return "medium"
        else:
            return "low"

    def _calculate_ml_severity(self, anomaly_score: float) -> str:
        """Calculate severity based on ML anomaly score"""
        if anomaly_score > 0.8:
            return "critical"
        elif anomaly_score > 0.6:
            return "high"
        elif anomaly_score > 0.4:
            return "medium"
        else:
            return "low"
