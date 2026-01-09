"""
Sample script to ingest raw data and retrieve features
"""
import requests
import json
from typing import List, Dict, Any

BASE_URL = "http://localhost:8000/api/v1"


def register_raw_table(name: str, description: str = None, schema: Dict[str, str] = None):
    """Register a raw table"""
    response = requests.post(
        f"{BASE_URL}/raw-tables",
        json={
            "name": name,
            "description": description,
            "schema_definition": schema
        }
    )
    response.raise_for_status()
    return response.json()


def create_feature(name: str, raw_table_id: int, computation_logic: str, entity_key: str, description: str = None):
    """Create a feature definition"""
    response = requests.post(
        f"{BASE_URL}/features",
        json={
            "name": name,
            "description": description,
            "raw_table_id": raw_table_id,
            "computation_logic": computation_logic,
            "entity_key": entity_key
        }
    )
    response.raise_for_status()
    return response.json()


def ingest_data(raw_table_id: int, data: List[Dict[str, Any]]):
    """Ingest raw data"""
    response = requests.post(
        f"{BASE_URL}/ingest",
        json={
            "raw_table_id": raw_table_id,
            "data": data
        }
    )
    response.raise_for_status()
    return response.json()


def compute_feature(feature_id: int, version: str, raw_data: List[Dict[str, Any]]):
    """Compute feature values"""
    response = requests.post(
        f"{BASE_URL}/features/{feature_id}/compute",
        json={
            "version": version,
            "raw_data": raw_data
        }
    )
    response.raise_for_status()
    return response.json()


def get_feature_vector(entity_id: str, feature_version_id: int = None, feature_name: str = None):
    """Retrieve feature vector for an entity"""
    params = {"entity_id": entity_id}
    if feature_version_id:
        params["feature_version_id"] = feature_version_id
    if feature_name:
        params["feature_name"] = feature_name
    
    response = requests.get(f"{BASE_URL}/feature-vectors", params=params)
    response.raise_for_status()
    return response.json()


def main():
    """Example workflow: ingest data and retrieve features"""
    print("=" * 60)
    print("Feature Store Sample Workflow")
    print("=" * 60)
    
    # Step 1: Register a raw table
    print("\n1. Registering raw table 'user_transactions'...")
    raw_table = register_raw_table(
        name="user_transactions",
        description="User transaction data",
        schema={
            "user_id": "string",
            "transaction_amount": "float",
            "transaction_date": "string",
            "category": "string"
        }
    )
    print(f"   ✓ Raw table created with ID: {raw_table['id']}")
    raw_table_id = raw_table['id']
    
    # Step 2: Ingest sample data
    print("\n2. Ingesting sample transaction data...")
    sample_data = [
        {
            "user_id": "user_001",
            "transaction_amount": 150.50,
            "transaction_date": "2024-01-15",
            "category": "groceries"
        },
        {
            "user_id": "user_001",
            "transaction_amount": 75.25,
            "transaction_date": "2024-01-16",
            "category": "restaurant"
        },
        {
            "user_id": "user_002",
            "transaction_amount": 200.00,
            "transaction_date": "2024-01-15",
            "category": "shopping"
        },
        {
            "user_id": "user_002",
            "transaction_amount": 50.00,
            "transaction_date": "2024-01-17",
            "category": "groceries"
        },
        {
            "user_id": "user_003",
            "transaction_amount": 300.75,
            "transaction_date": "2024-01-16",
            "category": "shopping"
        }
    ]
    ingest_result = ingest_data(raw_table_id, sample_data)
    print(f"   ✓ {ingest_result['records_ingested']} records ingested")
    
    # Step 3: Create a feature
    print("\n3. Creating feature 'avg_transaction_amount'...")
    feature = create_feature(
        name="avg_transaction_amount",
        raw_table_id=raw_table_id,
        computation_logic="SELECT user_id, AVG(transaction_amount) as avg_amount FROM data GROUP BY user_id",
        entity_key="user_id",
        description="Average transaction amount per user"
    )
    print(f"   ✓ Feature created with ID: {feature['id']}")
    feature_id = feature['id']
    
    # Step 4: Compute feature values
    print("\n4. Computing feature values (version v1)...")
    compute_result = compute_feature(feature_id, "v1", sample_data)
    print(f"   ✓ Feature version created with ID: {compute_result['feature_version_id']}")
    print(f"   ✓ {compute_result['vectors_created']} feature vectors created")
    print(f"   ✓ {compute_result['entities_processed']} entities processed")
    
    # Step 5: Retrieve feature vectors
    print("\n5. Retrieving feature vectors...")
    for user_id in ["user_001", "user_002", "user_003"]:
        try:
            vector = get_feature_vector(entity_id=user_id, feature_name="avg_transaction_amount")
            print(f"   ✓ {user_id}: {vector['feature_values']}")
        except Exception as e:
            print(f"   ✗ {user_id}: Error - {str(e)}")
    
    print("\n" + "=" * 60)
    print("Workflow completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the API server.")
        print("Please make sure the server is running on http://localhost:8000")
        print("Start it with: uvicorn app.main:app --reload")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
