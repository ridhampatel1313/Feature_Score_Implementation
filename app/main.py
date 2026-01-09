from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional
import pandas as pd

from app.database import get_db, init_db
from app.models import RawTable, Feature, FeatureVersion, FeatureVector
from app.schemas import (
    RawTableCreate, RawTableResponse,
    FeatureCreate, FeatureResponse,
    FeatureVersionCreate, FeatureVersionResponse,
    FeatureVectorRequest, FeatureVectorResponse,
    IngestDataRequest, IngestDataResponse,
    ComputeFeatureRequest
)
from app.consistency import ConsistencyChecker
from app.feature_computer import FeatureComputer
from app.cache import cache

app = FastAPI(
    title="Feature Store API",
    description="A simple feature store service for managing raw tables, computing features, and serving feature vectors",
    version="1.0.0"
)

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    init_db()


# ==================== Raw Table Endpoints ====================

@app.post("/api/v1/raw-tables", response_model=RawTableResponse, status_code=status.HTTP_201_CREATED)
async def register_raw_table(
    raw_table: RawTableCreate,
    db: Session = Depends(get_db)
):
    """Register a new raw table"""
    # Check if table already exists
    existing = db.query(RawTable).filter(RawTable.name == raw_table.name).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Raw table with name '{raw_table.name}' already exists"
        )
    
    db_raw_table = RawTable(
        name=raw_table.name,
        description=raw_table.description,
        schema_definition=raw_table.schema_definition
    )
    db.add(db_raw_table)
    db.commit()
    db.refresh(db_raw_table)
    
    return db_raw_table


@app.get("/api/v1/raw-tables", response_model=List[RawTableResponse])
async def list_raw_tables(db: Session = Depends(get_db)):
    """List all registered raw tables"""
    return db.query(RawTable).all()


@app.get("/api/v1/raw-tables/{table_id}", response_model=RawTableResponse)
async def get_raw_table(table_id: int, db: Session = Depends(get_db)):
    """Get a specific raw table by ID"""
    raw_table = db.query(RawTable).filter(RawTable.id == table_id).first()
    if not raw_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Raw table with ID {table_id} not found"
        )
    return raw_table


# ==================== Feature Endpoints ====================

@app.post("/api/v1/features", response_model=FeatureResponse, status_code=status.HTTP_201_CREATED)
async def create_feature(
    feature: FeatureCreate,
    db: Session = Depends(get_db)
):
    """Create a new feature definition"""
    # Validate raw table exists
    raw_table = db.query(RawTable).filter(RawTable.id == feature.raw_table_id).first()
    if not raw_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Raw table with ID {feature.raw_table_id} not found"
        )
    
    # Validate feature computation logic
    checker = ConsistencyChecker()
    db_feature = Feature(
        name=feature.name,
        description=feature.description,
        raw_table_id=feature.raw_table_id,
        computation_logic=feature.computation_logic,
        entity_key=feature.entity_key
    )
    db.add(db_feature)
    db.commit()
    db.refresh(db_feature)
    
    # Validate after creation
    is_valid, error_msg = checker.validate_feature_computation(db, db_feature.id)
    if not is_valid:
        db.delete(db_feature)
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid feature computation: {error_msg}"
        )
    
    return db_feature


@app.get("/api/v1/features", response_model=List[FeatureResponse])
async def list_features(db: Session = Depends(get_db)):
    """List all features"""
    return db.query(Feature).all()


@app.get("/api/v1/features/{feature_id}", response_model=FeatureResponse)
async def get_feature(feature_id: int, db: Session = Depends(get_db)):
    """Get a specific feature by ID"""
    feature = db.query(Feature).filter(Feature.id == feature_id).first()
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found"
        )
    return feature


# ==================== Feature Version Endpoints ====================

@app.post("/api/v1/features/{feature_id}/versions", response_model=FeatureVersionResponse, status_code=status.HTTP_201_CREATED)
async def create_feature_version(
    feature_id: int,
    version_data: FeatureVersionCreate,
    db: Session = Depends(get_db)
):
    """Create a new version for a feature"""
    # Validate feature exists
    feature = db.query(Feature).filter(Feature.id == feature_id).first()
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found"
        )
    
    # Check if version already exists
    existing = db.query(FeatureVersion).filter(
        FeatureVersion.feature_id == feature_id,
        FeatureVersion.version == version_data.version
    ).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Version '{version_data.version}' already exists for feature {feature_id}"
        )
    
    feature_version = FeatureVersion(
        feature_id=feature_id,
        version=version_data.version,
        status=version_data.status
    )
    db.add(feature_version)
    db.commit()
    db.refresh(feature_version)
    
    return feature_version


@app.post("/api/v1/features/{feature_id}/compute", status_code=status.HTTP_200_OK)
async def compute_feature(
    feature_id: int,
    request: ComputeFeatureRequest,
    db: Session = Depends(get_db)
):
    """Compute feature values for a given feature and version"""
    feature = db.query(Feature).filter(Feature.id == feature_id).first()
    if not feature:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature with ID {feature_id} not found"
        )
    
    # Validate raw data schema
    checker = ConsistencyChecker()
    is_valid, error_msg = checker.validate_raw_table_schema(db, feature.raw_table_id, request.raw_data)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid raw data: {error_msg}"
        )
    
    # Compute feature
    computer = FeatureComputer(db)
    try:
        result = computer.compute_feature(feature_id, request.version, request.raw_data)
        
        # Invalidate cache for this feature
        cache.clear_pattern(f"feature_vector:{feature_id}")
        
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error computing feature: {str(e)}"
        )


# ==================== Feature Vector Endpoints ====================

@app.get("/api/v1/feature-vectors", response_model=FeatureVectorResponse)
async def get_feature_vector(
    entity_id: str,
    feature_version_id: Optional[int] = None,
    feature_name: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get feature vector for a given entity"""
    # Check cache first
    cache_key = f"feature_vector:{entity_id}:{feature_version_id}:{feature_name}"
    cached_result = cache.get("feature_vector", entity_id, feature_version_id or "", feature_name or "")
    if cached_result:
        return cached_result
    
    # Determine which feature version to use
    if feature_version_id:
        feature_version = db.query(FeatureVersion).filter(FeatureVersion.id == feature_version_id).first()
        if not feature_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature version with ID {feature_version_id} not found"
            )
    elif feature_name:
        # Get latest version of feature with given name
        feature = db.query(Feature).filter(Feature.name == feature_name).first()
        if not feature:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature with name '{feature_name}' not found"
            )
        feature_version = db.query(FeatureVersion).filter(
            FeatureVersion.feature_id == feature.id
        ).order_by(FeatureVersion.created_at.desc()).first()
        if not feature_version:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No versions found for feature '{feature_name}'"
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either feature_version_id or feature_name must be provided"
        )
    
    # Get feature vector
    vector = db.query(FeatureVector).filter(
        FeatureVector.feature_version_id == feature_version.id,
        FeatureVector.entity_id == entity_id
    ).first()
    
    if not vector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Feature vector for entity '{entity_id}' not found"
        )
    
    # Check consistency
    checker = ConsistencyChecker()
    is_consistent, error_msg = checker.check_feature_version_consistency(db, feature_version.id)
    if not is_consistent:
        # Log warning but still return the vector
        print(f"Warning: Consistency check failed for feature version {feature_version.id}: {error_msg}")
    
    response = FeatureVectorResponse(
        entity_id=vector.entity_id,
        feature_values=vector.feature_values,
        feature_version_id=vector.feature_version_id,
        computed_at=vector.computed_at
    )
    
    # Cache the result
    cache.set("feature_vector", response.dict(), ttl=3600, entity_id, feature_version_id or "", feature_name or "")
    
    return response


# ==================== Data Ingestion Endpoint ====================

@app.post("/api/v1/ingest", response_model=IngestDataResponse)
async def ingest_data(
    ingest_request: IngestDataRequest,
    db: Session = Depends(get_db)
):
    """Ingest raw data into a registered table"""
    raw_table = db.query(RawTable).filter(RawTable.id == ingest_request.raw_table_id).first()
    if not raw_table:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Raw table with ID {ingest_request.raw_table_id} not found"
        )
    
    # Validate schema
    checker = ConsistencyChecker()
    is_valid, error_msg = checker.validate_raw_table_schema(db, ingest_request.raw_table_id, ingest_request.data)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid data: {error_msg}"
        )
    
    # In a real implementation, this would store data in the actual data storage
    # For now, we'll just validate and return success
    # The data would be stored in a separate data store (e.g., PostgreSQL table, Parquet files, etc.)
    
    return IngestDataResponse(
        message=f"Successfully ingested {len(ingest_request.data)} records into raw table '{raw_table.name}'",
        records_ingested=len(ingest_request.data)
    )


# ==================== Health Check ====================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
