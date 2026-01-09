import pandas as pd
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.models import RawTable, Feature, FeatureVersion, FeatureVector
from app.database import engine


class FeatureComputer:
    """Class for computing features from raw data"""
    
    def __init__(self, db: Session):
        self.db = db
        self.engine = engine
    
    def compute_feature(self, feature_id: int, version: str, raw_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Compute feature values for all entities
        
        Args:
            feature_id: ID of the feature to compute
            version: Version identifier for the feature
            raw_data: Optional raw data to use (if None, reads from database)
        
        Returns:
            Dictionary with computation results
        """
        feature = self.db.query(Feature).filter(Feature.id == feature_id).first()
        if not feature:
            raise ValueError(f"Feature {feature_id} not found")
        
        raw_table = self.db.query(RawTable).filter(RawTable.id == feature.raw_table_id).first()
        if not raw_table:
            raise ValueError(f"Raw table {feature.raw_table_id} not found")
        
        # Get or create feature version
        feature_version = self.db.query(FeatureVersion).filter(
            FeatureVersion.feature_id == feature_id,
            FeatureVersion.version == version
        ).first()
        
        if not feature_version:
            feature_version = FeatureVersion(
                feature_id=feature_id,
                version=version,
                status="active"
            )
            self.db.add(feature_version)
            self.db.commit()
            self.db.refresh(feature_version)
        
        # Load raw data
        if raw_data is None:
            # In a real implementation, this would read from the actual data storage
            # For now, we'll use a placeholder
            raise ValueError("Raw data must be provided for computation")
        
        df = pd.DataFrame(raw_data)
        
        # Validate entity_key exists
        if feature.entity_key not in df.columns:
            raise ValueError(f"Entity key '{feature.entity_key}' not found in data")
        
        # Execute computation logic (assuming SQL-like query)
        # In production, this would be more sophisticated
        try:
            # For SQL queries, we'd execute them against the data
            # For now, we'll do a simple aggregation example
            if "SELECT" in feature.computation_logic.upper():
                # Execute SQL query (simplified - in production use proper SQL execution)
                result_df = self._execute_sql_query(df, feature.computation_logic, feature.entity_key)
            else:
                # Assume it's a Python expression or aggregation
                result_df = self._execute_python_logic(df, feature.computation_logic, feature.entity_key)
            
            # Store feature vectors
            vectors_created = 0
            for _, row in result_df.iterrows():
                entity_id = str(row[feature.entity_key])
                feature_values = {k: v for k, v in row.items() if k != feature.entity_key}
                
                # Check if vector already exists
                existing = self.db.query(FeatureVector).filter(
                    FeatureVector.feature_version_id == feature_version.id,
                    FeatureVector.entity_id == entity_id
                ).first()
                
                if existing:
                    existing.feature_values = feature_values
                else:
                    vector = FeatureVector(
                        feature_version_id=feature_version.id,
                        entity_id=entity_id,
                        feature_values=feature_values
                    )
                    self.db.add(vector)
                    vectors_created += 1
            
            self.db.commit()
            
            return {
                "feature_version_id": feature_version.id,
                "vectors_created": vectors_created,
                "entities_processed": len(result_df)
            }
        
        except Exception as e:
            self.db.rollback()
            raise ValueError(f"Error computing feature: {str(e)}")
    
    def _execute_sql_query(self, df: pd.DataFrame, query: str, entity_key: str) -> pd.DataFrame:
        """Execute SQL-like query on DataFrame"""
        # Simplified SQL execution - in production, use proper SQL engine
        # For now, we'll parse basic SELECT statements
        query_upper = query.upper().strip()
        
        if query_upper.startswith("SELECT"):
            # Basic SELECT with GROUP BY
            if "GROUP BY" in query_upper:
                # Extract GROUP BY column and aggregation
                # This is a simplified parser
                group_by_col = entity_key
                # For now, return grouped data
                return df.groupby(entity_key).agg({
                    col: 'mean' if df[col].dtype in ['int64', 'float64'] else 'first'
                    for col in df.columns if col != entity_key
                }).reset_index()
            else:
                return df
        
        return df
    
    def _execute_python_logic(self, df: pd.DataFrame, logic: str, entity_key: str) -> pd.DataFrame:
        """Execute Python-based computation logic"""
        # In production, this would use a sandboxed Python execution environment
        # For now, we'll do simple aggregations
        if "mean" in logic.lower() or "avg" in logic.lower():
            return df.groupby(entity_key).mean().reset_index()
        elif "sum" in logic.lower():
            return df.groupby(entity_key).sum().reset_index()
        elif "count" in logic.lower():
            return df.groupby(entity_key).size().reset_index(name='count')
        else:
            # Default: return grouped by entity
            return df.groupby(entity_key).first().reset_index()
