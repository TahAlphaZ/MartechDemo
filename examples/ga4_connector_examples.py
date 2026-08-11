"""
Example: Using the Google Analytics 4 Connector

This script demonstrates how to use the GA4 connector in various scenarios:
- Basic data extraction
- Custom date ranges
- Schema inspection
- Error handling
"""

import json
from datetime import datetime, timedelta
from src.connectors.connector_factory import load_registry, get_active_connectors, create_connector
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def example_1_basic_extraction():
    """Example 1: Extract GA4 data with default configuration"""
    print("\n=== Example 1: Basic Extraction ===")
    
    # Load connector registry
    registry = load_registry()
    active_connectors = get_active_connectors(registry)
    
    # Get GA4 connector config
    ga_config = active_connectors['analytics_google_analytics']
    print(f"Connector: {ga_config.name}")
    print(f"Landing Path: {ga_config.landing_path}")
    print(f"Property ID: {ga_config.extra.get('property_id')}")
    
    # In production, retrieve secret from Key Vault
    # (simplified for example; actual retrieval shown in example_2)
    service_account_json = get_service_account_from_keyvault(
        ga_config.keyvault_secret
    )
    
    # Create connector instance
    connector = create_connector(ga_config, service_account_json)
    
    # Validate connection to GA4
    if connector.validate_connection():
        print("✓ Connection validated")
        
        # Extract data (defaults to last 1 day)
        data = connector.extract()
        print(f"✓ Extracted {len(data)} rows")
        
        # Print first row as example
        if data:
            print(f"Sample row: {data[0]}")
    else:
        print("✗ Connection failed")


def example_2_custom_date_range():
    """Example 2: Extract GA4 data for a custom date range"""
    print("\n=== Example 2: Custom Date Range ===")
    
    registry = load_registry()
    active_connectors = get_active_connectors(registry)
    ga_config = active_connectors['analytics_google_analytics']
    
    service_account_json = get_service_account_from_keyvault(
        ga_config.keyvault_secret
    )
    
    connector = create_connector(ga_config, service_account_json)
    
    # Define custom date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    print(f"Extracting data from {start_date} to {end_date}")
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "date_range_days": 30
    }
    
    data = connector.extract(params=params)
    print(f"✓ Extracted {len(data)} rows for 30-day period")


def example_3_schema_inspection():
    """Example 3: Inspect connector schema and metadata"""
    print("\n=== Example 3: Schema Inspection ===")
    
    registry = load_registry()
    active_connectors = get_active_connectors(registry)
    ga_config = active_connectors['analytics_google_analytics']
    
    service_account_json = get_service_account_from_keyvault(
        ga_config.keyvault_secret
    )
    
    connector = create_connector(ga_config, service_account_json)
    
    # Get schema
    schema = connector.get_schema()
    print("Schema:")
    print(f"  Dimensions: {schema['dimensions']}")
    print(f"  Metrics: {schema['metrics']}")
    print(f"  Sample Row: {schema['sample_row']}")
    
    # Get metadata
    metadata = connector.get_metadata()
    print("\nMetadata:")
    for key, value in metadata.items():
        print(f"  {key}: {value}")


def example_4_error_handling():
    """Example 4: Error handling and recovery"""
    print("\n=== Example 4: Error Handling ===")
    
    try:
        registry = load_registry()
        active_connectors = get_active_connectors(registry)
        ga_config = active_connectors['analytics_google_analytics']
        
        # Attempt with invalid credentials (for demo)
        invalid_json = json.dumps({"invalid": "key"})
        
        try:
            connector = create_connector(ga_config, invalid_json)
            connector.validate_connection()
        except Exception as e:
            print(f"✗ Expected error (invalid credentials): {str(e)[:100]}")
        
        # Proper credentials would be retrieved here
        print("✓ Error handling works as expected")
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")


def example_5_batch_processing():
    """Example 5: Batch extract and transform GA4 data"""
    print("\n=== Example 5: Batch Processing ===")
    
    registry = load_registry()
    active_connectors = get_active_connectors(registry)
    ga_config = active_connectors['analytics_google_analytics']
    
    service_account_json = get_service_account_from_keyvault(
        ga_config.keyvault_secret
    )
    
    connector = create_connector(ga_config, service_account_json)
    
    # Extract data
    raw_data = connector.extract()
    
    # Transform into structured format
    transformed = []
    for row in raw_data:
        dims = row.get("dimensions", [])
        metrics = row.get("metrics", [[]])[0] if row.get("metrics") else []
        
        if len(dims) >= 3 and len(metrics) >= 4:
            transformed.append({
                "date": dims[0],
                "source": dims[1],
                "medium": dims[2],
                "sessions": metrics[0],
                "users": metrics[1],
                "conversions": metrics[2],
                "revenue": metrics[3],
            })
    
    print(f"Transformed {len(transformed)} rows")
    if transformed:
        print(f"Sample: {transformed[0]}")


def example_6_incremental_extraction():
    """Example 6: Incremental extraction (today's data only)"""
    print("\n=== Example 6: Incremental Extraction ===")
    
    registry = load_registry()
    active_connectors = get_active_connectors(registry)
    ga_config = active_connectors['analytics_google_analytics']
    
    service_account_json = get_service_account_from_keyvault(
        ga_config.keyvault_secret
    )
    
    connector = create_connector(ga_config, service_account_json)
    
    # Extract only today's data
    today = datetime.now().date()
    params = {
        "start_date": today,
        "end_date": today,
        "date_range_days": 1
    }
    
    data = connector.extract(params=params)
    print(f"Extracted {len(data)} rows for today ({today})")


def get_service_account_from_keyvault(secret_name: str) -> str:
    """
    Retrieve service account JSON from Azure Key Vault.
    
    In production, this is called by the pipeline framework.
    Set environment variables:
    - KEYVAULT_NAME: Name of your Azure Key Vault
    - AZURE_TENANT_ID: Azure tenant ID
    - AZURE_CLIENT_ID: Service principal client ID
    - AZURE_CLIENT_SECRET: Service principal secret
    """
    import os
    
    keyvault_name = os.getenv("KEYVAULT_NAME", "your-keyvault-name")
    vault_url = f"https://{keyvault_name}.vault.azure.net/"
    
    try:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=vault_url, credential=credential)
        secret = client.get_secret(secret_name)
        return secret.value
    except Exception as e:
        print(f"✗ Failed to retrieve secret '{secret_name}': {e}")
        raise


if __name__ == "__main__":
    print("Google Analytics 4 Connector Examples")
    print("=" * 50)
    
    try:
        # Uncomment examples to run
        example_1_basic_extraction()
        # example_2_custom_date_range()
        # example_3_schema_inspection()
        # example_4_error_handling()
        # example_5_batch_processing()
        # example_6_incremental_extraction()
        
    except Exception as e:
        print(f"\n✗ Example failed: {e}")
        import traceback
        traceback.print_exc()
