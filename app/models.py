from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, Float, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.database import Base


class RawTable(Base):
    """Model for registered raw data tables"""
    __tablename__ = "raw_tables"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    description = Column(Text, nullable=True)
    schema_definition = Column(JSON, nullable=True)  # Store column names and types
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    features = relationship("Feature", back_populates="raw_table", cascade="all, delete-orphan")


class Feature(Base):
    """Model for feature definitions"""
    __tablename__ = "features"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    description = Column(Text, nullable=True)
    raw_table_id = Column(Integer, ForeignKey("raw_tables.id"), nullable=False)
    computation_logic = Column(Text, nullable=False)  # SQL or Python code for feature computation
    entity_key = Column(String, nullable=False)  # Column name that identifies entities
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    raw_table = relationship("RawTable", back_populates="features")
    versions = relationship("FeatureVersion", back_populates="feature", cascade="all, delete-orphan")


class FeatureVersion(Base):
    """Model for feature versions"""
    __tablename__ = "feature_versions"
    
    id = Column(Integer, primary_key=True, index=True)
    feature_id = Column(Integer, ForeignKey("features.id"), nullable=False)
    version = Column(String, nullable=False)  # e.g., "v1", "v2", "latest"
    status = Column(String, default="active")  # active, deprecated, archived
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    feature = relationship("Feature", back_populates="versions")
    vectors = relationship("FeatureVector", back_populates="feature_version", cascade="all, delete-orphan")
    
    __table_args__ = ({"sqlite_autoincrement": True},)


class FeatureVector(Base):
    """Model for computed feature vectors"""
    __tablename__ = "feature_vectors"
    
    id = Column(Integer, primary_key=True, index=True)
    feature_version_id = Column(Integer, ForeignKey("feature_versions.id"), nullable=False)
    entity_id = Column(String, index=True, nullable=False)  # The entity identifier
    feature_values = Column(JSON, nullable=False)  # Dictionary of feature name -> value
    computed_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    feature_version = relationship("FeatureVersion", back_populates="vectors")
    
    __table_args__ = ({"sqlite_autoincrement": True},)
