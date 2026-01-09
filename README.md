# Feature Store Service

A simple feature store service built with FastAPI that provides APIs to register raw tables, compute and version features, and serve feature vectors for given entities. Includes basic consistency checks and caching.

## Features

- **Raw Table Registration**: Register and manage raw data tables with schema definitions
- **Feature Management**: Define features with computation logic and entity keys
- **Feature Versioning**: Create and manage multiple versions of features
- **Feature Computation**: Compute feature values from raw data
- **Feature Vector Serving**: Retrieve feature vectors for specific entities
- **Consistency Checks**: Validate data schemas and feature computation logic
- **Caching**: Redis-based caching with in-memory fallback for feature vectors
- **Data Ingestion**: Ingest raw data into registered tables

## Tech Stack

- **Python 3.9+**
- **FastAPI**: Modern, fast web framework for building APIs
- **SQLAlchemy**: SQL toolkit and ORM
- **SQLite**: Database (can be easily switched to PostgreSQL)
- **Pandas**: Data manipulation and analysis
- **Redis**: Caching (optional, falls back to in-memory cache)

## Installation

1. Clone the repository and navigate to the project directory:
```bash
cd reaidy_ai_task
```

2. Create a virtual environment (recommended):
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. (Optional) Set up Redis for caching:
```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or install Redis locally
# macOS: brew install redis
# Ubuntu: sudo apt-get install redis-server
```

## Configuration

Create a `.env` file (optional) to configure the database and Redis:

```env
DATABASE_URL=sqlite:///./feature_store.db
REDIS_URL=redis://localhost:6379/0
```

If not provided, defaults will be used:
- Database: SQLite at `./feature_store.db`
- Redis: Will attempt to connect to `redis://localhost:6379/0`, falls back to in-memory cache if unavailable

## Running the Service

Start the FastAPI server:

```bash
uvicorn app.main:app --reload
```

The API will be available at:
- API: http://localhost:8000
- Interactive API docs: http://localhost:8000/docs
- Alternative API docs: http://localhost:8000/redoc

## API Documentation

### Raw Tables

#### Register Raw Table
```http
POST /api/v1/raw-tables
Content-Type: application/json

{
  "name": "user_transactions",
  "description": "User transaction data",
  "schema_definition": {
    "user_id": "string",
    "transaction_amount": "float",
    "transaction_date": "string"
  }
}
```

#### List Raw Tables
```http
GET /api/v1/raw-tables
```

#### Get Raw Table
```http
GET /api/v1/raw-tables/{table_id}
```

### Features

#### Create Feature
```http
POST /api/v1/features
Content-Type: application/json

{
  "name": "avg_transaction_amount",
  "description": "Average transaction amount per user",
  "raw_table_id": 1,
  "computation_logic": "SELECT user_id, AVG(transaction_amount) as avg_amount FROM data GROUP BY user_id",
  "entity_key": "user_id"
}
```

#### List Features
```http
GET /api/v1/features
```

#### Get Feature
```http
GET /api/v1/features/{feature_id}
```

### Feature Versions

#### Create Feature Version
```http
POST /api/v1/features/{feature_id}/versions
Content-Type: application/json

{
  "version": "v1",
  "status": "active"
}
```

#### Compute Feature
```http
POST /api/v1/features/{feature_id}/compute?version=v1
Content-Type: application/json

[
  {
    "user_id": "user_001",
    "transaction_amount": 150.50,
    "transaction_date": "2024-01-15"
  },
  ...
]
```

### Feature Vectors

#### Get Feature Vector
```http
GET /api/v1/feature-vectors?entity_id=user_001&feature_name=avg_transaction_amount
```

Or with specific version:
```http
GET /api/v1/feature-vectors?entity_id=user_001&feature_version_id=1
```

### Data Ingestion

#### Ingest Data
```http
POST /api/v1/ingest
Content-Type: application/json

{
  "raw_table_id": 1,
  "data": [
    {
      "user_id": "user_001",
      "transaction_amount": 150.50,
      "transaction_date": "2024-01-15"
    },
    ...
  ]
}
```

## Sample Script

Run the sample ingestion script to see the feature store in action:

```bash
# Make sure the server is running first
python sample_ingestion.py
```

This script demonstrates:
1. Registering a raw table
2. Ingesting sample data
3. Creating a feature
4. Computing feature values
5. Retrieving feature vectors

## Database Schema

The service uses the following main tables:

- **raw_tables**: Stores registered raw table definitions
- **features**: Stores feature definitions with computation logic
- **feature_versions**: Stores version information for features
- **feature_vectors**: Stores computed feature vectors for entities

## Consistency Checks

The service performs several consistency checks:

1. **Schema Validation**: Validates ingested data against registered table schemas
2. **Feature Validation**: Validates feature computation logic and entity keys
3. **Version Consistency**: Checks that feature vectors have consistent structure
4. **Entity Validation**: Verifies entity existence before serving vectors

## Caching

Feature vectors are cached for 1 hour (configurable) to improve performance:
- Uses Redis if available
- Falls back to in-memory cache if Redis is unavailable
- Cache is automatically invalidated when features are recomputed

## Error Handling

The API returns appropriate HTTP status codes:
- `200 OK`: Successful request
- `201 Created`: Resource created successfully
- `400 Bad Request`: Invalid input or validation error
- `404 Not Found`: Resource not found
- `500 Internal Server Error`: Server error

## Development

### Project Structure

```
reaidy_ai_task/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application and routes
│   ├── database.py          # Database configuration
│   ├── models.py            # SQLAlchemy models
│   ├── schemas.py           # Pydantic schemas
│   ├── consistency.py       # Consistency checking logic
│   ├── feature_computer.py  # Feature computation logic
│   └── cache.py             # Caching implementation
├── sample_ingestion.py      # Sample script
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

### Switching to PostgreSQL

To use PostgreSQL instead of SQLite:

1. Install PostgreSQL adapter:
```bash
pip install psycopg2-binary
```

2. Update `.env`:
```env
DATABASE_URL=postgresql://user:password@localhost/feature_store
```

3. The service will automatically use PostgreSQL.

## License

This project is provided as-is for demonstration purposes.
