from datetime import date, datetime
from typing import List, Dict, Optional, Tuple, Union
import pandas as pd
from sqlalchemy import text, and_
from sqlalchemy.orm import Session

from app.models import BurstDetection, BurstMethod
from app.models.burst_point import BurstPoint

class BurstAnalysisUtils:
    """
    Utility class for dynamic burst analysis using BurstPoint data.
    Enables score computation within specific time ranges and detailed burst analysis.
    """

    @staticmethod
    def compute_dynamic_score(
        db: Session,
        ngram_id: int,
        method: BurstMethod,
        start_date: Union[date, str],
        end_date: Union[date, str]
    ) -> Dict:
        """
        Compute burst score for a specific ngram and method within a time range.
        
        Args:
            db: Database session
            ngram_id: ID of the ngram
            method: Burst detection method
            start_date: Start date of analysis window
            end_date: End date of analysis window
            
        Returns:
            Dict with computed metrics for the time range
        """
        # Convert string dates to date objects if needed
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()

        # Query points within the date range
        points_query = """
            SELECT 
                date,
                period_index,
                contribution,
                raw_value,
                baseline_value,
                poisson_z_score,
                macd_histogram_value,
                kleinberg_state,
                state_probability,
                weight_contribution
            FROM burst_points
            WHERE ngram_id = :ngram_id 
              AND method = :method
              AND date >= :start_date 
              AND date <= :end_date
            ORDER BY date
        """

        points = db.execute(text(points_query), {
            "ngram_id": ngram_id,
            "method": method.value,
            "start_date": start_date,
            "end_date": end_date
        }).mappings().all()

        if not points:
            return {
                "ngram_id": ngram_id,
                "method": method.value,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "dynamic_score": 0.0,
                "point_count": 0,
                "date_range_days": (end_date - start_date).days + 1,
                "raw_metrics": {}
            }

        # Compute metrics
        points_df = pd.DataFrame(points)
        total_contribution = points_df['contribution'].sum()
        point_count = len(points_df)
        date_range_days = (end_date - start_date).days + 1

        # Method-specific metrics
        raw_metrics = {
            "total_raw_value": points_df['raw_value'].sum(),
            "avg_raw_value": points_df['raw_value'].mean(),
            "max_raw_value": points_df['raw_value'].max(),
        }

        if method == BurstMethod.MACD:
            raw_metrics.update({
                "avg_poisson_z": points_df['poisson_z_score'].mean(),
                "max_poisson_z": points_df['poisson_z_score'].max(),
                "avg_macd_histogram": points_df['macd_histogram_value'].mean(),
                "max_macd_histogram": points_df['macd_histogram_value'].max(),
                "points_above_threshold": (points_df['poisson_z_score'] > 2.0).sum(),
            })
        elif method == BurstMethod.KLEINBERG:
            raw_metrics.update({
                "avg_state_probability": points_df['state_probability'].mean(),
                "max_state_probability": points_df['state_probability'].max(),
                "burst_state_ratio": (points_df['kleinberg_state'] == 1).mean(),
                "avg_weight_contribution": points_df['weight_contribution'].mean(),
            })

        return {
            "ngram_id": ngram_id,
            "method": method.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "dynamic_score": float(total_contribution),
            "point_count": point_count,
            "date_range_days": date_range_days,
            "points_per_day": point_count / date_range_days if date_range_days > 0 else 0,
            "raw_metrics": raw_metrics,
            "daily_breakdown": BurstAnalysisUtils._get_daily_breakdown(points_df)
        }

    @staticmethod
    def _get_daily_breakdown(points_df: pd.DataFrame) -> List[Dict]:
        """Get daily breakdown of point contributions."""
        if points_df.empty:
            return []
        
        daily_stats = points_df.groupby('date').agg({
            'contribution': ['sum', 'count', 'mean'],
            'raw_value': ['sum', 'mean', 'max'],
        }).round(4)

        # Flatten column names
        daily_stats.columns = [f"{col[0]}_{col[1]}" for col in daily_stats.columns]
        daily_stats = daily_stats.reset_index()

        return [
            {
                "date": row['date'].isoformat() if hasattr(row['date'], 'isoformat') else str(row['date']),
                "total_contribution": row['contribution_sum'],
                "point_count": int(row['contribution_count']),
                "avg_contribution": row['contribution_mean'],
                "total_raw_value": row['raw_value_sum'],
                "avg_raw_value": row['raw_value_mean'],
                "max_raw_value": row['raw_value_max']
            }
            for _, row in daily_stats.iterrows()
        ]

    @staticmethod
    def get_points_in_date_range(
        db: Session,
        ngram_id: int,
        method: BurstMethod,
        start_date: Union[date, str],
        end_date: Union[date, str],
        min_contribution: Optional[float] = None
    ) -> List[Dict]:
        """
        Get detailed point data within a specific date range.
        
        Args:
            db: Database session
            ngram_id: ID of the ngram
            method: Burst detection method
            start_date: Start date
            end_date: End date
            min_contribution: Minimum contribution threshold
            
        Returns:
            List of point records with full details
        """
        # Convert string dates to date objects if needed
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()

        query = """
            SELECT 
                bp.*,
                n.text as ngram,
                d.name as domain,
                f.name as field,
                s.name as subfield
            FROM burst_points bp
            JOIN ngrams n ON bp.ngram_id = n.id
            JOIN subfields s ON n.subfield_id = s.id
            JOIN fields f ON s.field_id = f.id
            JOIN domains d ON f.domain_id = d.id
            WHERE bp.ngram_id = :ngram_id 
              AND bp.method = :method
              AND bp.date >= :start_date 
              AND bp.date <= :end_date
        """

        params = {
            "ngram_id": ngram_id,
            "method": method.value,
            "start_date": start_date,
            "end_date": end_date
        }

        if min_contribution is not None:
            query += " AND bp.contribution >= :min_contribution"
            params["min_contribution"] = min_contribution

        query += " ORDER BY bp.date, bp.period_index"

        results = db.execute(text(query), params).mappings().all()
        return [dict(row) for row in results]

    @staticmethod
    def compare_methods_for_ngram(
        db: Session,
        ngram_id: int,
        start_date: Union[date, str],
        end_date: Union[date, str]
    ) -> Dict:
        """
        Compare Kleinberg and MACD results for the same ngram within a date range.
        
        Returns:
            Dict with comparison metrics
        """
        kleinberg_metrics = BurstAnalysisUtils.compute_dynamic_score(
            db, ngram_id, BurstMethod.KLEINBERG, start_date, end_date
        )
        macd_metrics = BurstAnalysisUtils.compute_dynamic_score(
            db, ngram_id, BurstMethod.MACD, start_date, end_date
        )

        # Calculate comparison ratios
        score_ratio = (
            macd_metrics["dynamic_score"] / kleinberg_metrics["dynamic_score"]
            if kleinberg_metrics["dynamic_score"] > 0 else float('inf')
        )
        
        point_density_ratio = (
            macd_metrics["points_per_day"] / kleinberg_metrics["points_per_day"]
            if kleinberg_metrics["points_per_day"] > 0 else float('inf')
        )

        return {
            "ngram_id": ngram_id,
            "date_range": {
                "start_date": kleinberg_metrics["start_date"],
                "end_date": kleinberg_metrics["end_date"]
            },
            "kleinberg": kleinberg_metrics,
            "macd": macd_metrics,
            "comparison": {
                "score_ratio_macd_to_kleinberg": score_ratio,
                "score_difference": macd_metrics["dynamic_score"] - kleinberg_metrics["dynamic_score"],
                "point_density_ratio": point_density_ratio,
                "method_agreement": score_ratio if 0.5 <= score_ratio <= 2.0 else "divergent"
            }
        }

    @staticmethod
    def get_top_contributors_by_date(
        db: Session,
        ngram_id: int,
        method: BurstMethod,
        target_date: Union[date, str],
        limit: int = 10
    ) -> List[Dict]:
        """
        Get the top contributing time points for a specific date.
        
        Args:
            db: Database session
            ngram_id: ID of the ngram
            method: Burst detection method
            target_date: Specific date to analyze
            limit: Maximum number of points to return
            
        Returns:
            List of top contributing points
        """
        if isinstance(target_date, str):
            target_date = pd.to_datetime(target_date).date()

        query = """
            SELECT 
                bp.*,
                n.text as ngram
            FROM burst_points bp
            JOIN ngrams n ON bp.ngram_id = n.id
            WHERE bp.ngram_id = :ngram_id
              AND bp.method = :method
              AND bp.date = :target_date
            ORDER BY bp.contribution DESC
            LIMIT :limit
        """

        results = db.execute(text(query), {
            "ngram_id": ngram_id,
            "method": method.value,
            "target_date": target_date,
            "limit": limit
        }).mappings().all()

        return [dict(row) for row in results]

    @staticmethod
    def aggregate_scores_by_time_window(
        db: Session,
        ngram_ids: List[int],
        method: BurstMethod,
        start_date: Union[date, str],
        end_date: Union[date, str],
        window_size_days: int = 30
    ) -> Dict:
        """
        Aggregate burst scores across multiple ngrams using rolling time windows.
        
        Args:
            db: Database session
            ngram_ids: List of ngram IDs to analyze
            method: Burst detection method
            start_date: Analysis start date
            end_date: Analysis end date
            window_size_days: Size of rolling window in days
            
        Returns:
            Dict with aggregated metrics by time window
        """
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()

        # Get all points for the ngrams in the date range
        query = """
            SELECT 
                ngram_id,
                date,
                contribution,
                raw_value
            FROM burst_points
            WHERE ngram_id = ANY(:ngram_ids)
              AND method = :method
              AND date >= :start_date
              AND date <= :end_date
            ORDER BY date
        """

        results = db.execute(text(query), {
            "ngram_ids": ngram_ids,
            "method": method.value,
            "start_date": start_date,
            "end_date": end_date
        }).mappings().all()

        if not results:
            return {"error": "No data found for the specified criteria"}

        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])

        # Create rolling windows
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        windows = []

        for window_start in date_range[::window_size_days]:
            window_end = min(window_start + pd.Timedelta(days=window_size_days-1), pd.to_datetime(end_date))
            
            window_data = df[(df['date'] >= window_start) & (df['date'] <= window_end)]
            
            if len(window_data) > 0:
                windows.append({
                    "window_start": window_start.date().isoformat(),
                    "window_end": window_end.date().isoformat(),
                    "total_contribution": window_data['contribution'].sum(),
                    "avg_contribution": window_data['contribution'].mean(),
                    "point_count": len(window_data),
                    "unique_ngrams": window_data['ngram_id'].nunique(),
                    "total_raw_value": window_data['raw_value'].sum(),
                })

        return {
            "method": method.value,
            "analysis_period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "window_size_days": window_size_days
            },
            "total_ngrams": len(ngram_ids),
            "windows": windows,
            "summary": {
                "total_windows": len(windows),
                "avg_contribution_per_window": sum(w["total_contribution"] for w in windows) / len(windows) if windows else 0,
                "peak_window": max(windows, key=lambda w: w["total_contribution"]) if windows else None
            }
        }