from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class RawTableCreate(BaseModel):
    name: str = Field(..., description="Name of the raw table")
    description: Optional[str] = None
    schema_definition: Optional[Dict[str, str]] = Field(None, description="Column names and their types")


class RawTableResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    schema_definition: Optional[Dict[str, str]]
    created_at: datetime
    
    class Config:
        from_attributes = True


class FeatureCreate(BaseModel):
    name: str = Field(..., description="Name of the feature")
    description: Optional[str] = None
    raw_table_id: int = Field(..., description="ID of the raw table")
    computation_logic: str = Field(..., description="SQL query or computation logic")
    entity_key: str = Field(..., description="Column name that identifies entities")


class FeatureResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    raw_table_id: int
    computation_logic: str
    entity_key: str
    created_at: datetime
    
    class Config:
        from_attributes = True


class FeatureVersionCreate(BaseModel):
    feature_id: int = Field(..., description="ID of the feature")
    version: str = Field(..., description="Version identifier (e.g., 'v1', 'v2')")
    status: Optional[str] = Field("active", description="Status: active, deprecated, archived")


class FeatureVersionResponse(BaseModel):
    id: int
    feature_id: int
    version: str
    status: str
    created_at: datetime
    
    class Config:
        from_attributes = True


class FeatureVectorRequest(BaseModel):
    entity_id: str = Field(..., description="Entity identifier")
    feature_version_id: Optional[int] = Field(None, description="Specific feature version ID")
    feature_name: Optional[str] = Field(None, description="Feature name (uses latest version if version_id not provided)")


class FeatureVectorResponse(BaseModel):
    entity_id: str
    feature_values: Dict[str, Any]
    feature_version_id: int
    computed_at: datetime
    
    class Config:
        from_attributes = True


class IngestDataRequest(BaseModel):
    raw_table_id: int = Field(..., description="ID of the raw table")
    data: List[Dict[str, Any]] = Field(..., description="List of records to ingest")


class IngestDataResponse(BaseModel):
    message: str
    records_ingested: int


class ComputeFeatureRequest(BaseModel):
    version: str = Field(..., description="Version identifier (e.g., 'v1', 'v2')")
    raw_data: List[Dict[str, Any]] = Field(..., description="Raw data to compute features from")
