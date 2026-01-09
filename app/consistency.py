from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.orm import Session
from app.models import RawTable, Feature, FeatureVersion, FeatureVector
import pandas as pd


class ConsistencyChecker:
    """Class for performing consistency checks on the feature store"""
    
    @staticmethod
    def validate_raw_table_schema(db: Session, raw_table_id: int, data: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        """Validate that ingested data matches the raw table schema"""
        raw_table = db.query(RawTable).filter(RawTable.id == raw_table_id).first()
        if not raw_table:
            return False, "Raw table not found"
        
        if not raw_table.schema_definition:
            return True, None  # No schema defined, accept all data
        
        schema = raw_table.schema_definition
        if not data:
            return False, "No data provided"
        
        # Check that all required columns exist
        required_columns = set(schema.keys())
        data_columns = set(data[0].keys())
        
        missing_columns = required_columns - data_columns
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}"
        
        # Check data types (basic validation)
        for record in data:
            for col, expected_type in schema.items():
                if col in record:
                    value = record[col]
                    if value is not None:
                        # Basic type checking
                        if expected_type == "integer" and not isinstance(value, int):
                            try:
                                int(value)
                            except (ValueError, TypeError):
                                return False, f"Column {col} should be {expected_type}, got {type(value).__name__}"
                        elif expected_type == "float" and not isinstance(value, (int, float)):
                            try:
                                float(value)
                            except (ValueError, TypeError):
                                return False, f"Column {col} should be {expected_type}, got {type(value).__name__}"
                        elif expected_type == "string" and not isinstance(value, str):
                            return False, f"Column {col} should be {expected_type}, got {type(value).__name__}"
        
        return True, None
    
    @staticmethod
    def validate_feature_computation(db: Session, feature_id: int) -> Tuple[bool, Optional[str]]:
        """Validate that feature computation logic is valid"""
        feature = db.query(Feature).filter(Feature.id == feature_id).first()
        if not feature:
            return False, "Feature not found"
        
        raw_table = db.query(RawTable).filter(RawTable.id == feature.raw_table_id).first()
        if not raw_table:
            return False, "Raw table not found for feature"
        
        # Check that entity_key exists in schema
        if raw_table.schema_definition:
            if feature.entity_key not in raw_table.schema_definition:
                return False, f"Entity key '{feature.entity_key}' not found in raw table schema"
        
        # Basic SQL validation (check for SQL injection patterns)
        computation = feature.computation_logic.lower()
        dangerous_keywords = ["drop", "delete", "truncate", "alter", "create", "insert", "update"]
        for keyword in dangerous_keywords:
            if f" {keyword} " in computation or computation.startswith(keyword):
                return False, f"Computation logic contains potentially dangerous keyword: {keyword}"
        
        return True, None
    
    @staticmethod
    def check_feature_version_consistency(db: Session, feature_version_id: int) -> Tuple[bool, Optional[str]]:
        """Check consistency of feature version data"""
        feature_version = db.query(FeatureVersion).filter(FeatureVersion.id == feature_version_id).first()
        if not feature_version:
            return False, "Feature version not found"
        
        feature = db.query(Feature).filter(Feature.id == feature_version.feature_id).first()
        if not feature:
            return False, "Feature not found"
        
        # Check that feature vectors have consistent structure
        vectors = db.query(FeatureVector).filter(FeatureVector.feature_version_id == feature_version_id).limit(10).all()
        if vectors:
            # Check that all vectors have the same keys
            first_keys = set(vectors[0].feature_values.keys())
            for vector in vectors[1:]:
                if set(vector.feature_values.keys()) != first_keys:
                    return False, "Feature vectors have inconsistent structure"
        
        return True, None
    
    @staticmethod
    def validate_entity_exists(db: Session, raw_table_id: int, entity_id: str, entity_key: str) -> bool:
        """Check if entity exists in raw table (requires actual data storage)"""
        # This would require querying the actual data storage
        # For now, we'll assume entities exist if feature vectors exist
        feature = db.query(Feature).filter(
            Feature.raw_table_id == raw_table_id,
            Feature.entity_key == entity_key
        ).first()
        
        if feature:
            feature_version = db.query(FeatureVersion).filter(
                FeatureVersion.feature_id == feature.id
            ).order_by(FeatureVersion.created_at.desc()).first()
            
            if feature_version:
                vector = db.query(FeatureVector).filter(
                    FeatureVector.feature_version_id == feature_version.id,
                    FeatureVector.entity_id == entity_id
                ).first()
                return vector is not None
        
        return False
