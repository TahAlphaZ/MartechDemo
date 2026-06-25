"""
Example: Using the HubSpot Connector
Demonstrates how to fetch contacts, companies, deals from HubSpot.
"""
from src.connectors.connector_factory import create_connector
from src.connectors.base_connector import ConnectorConfig


def main():
    """Example usage of HubSpot connector."""

    # Configure HubSpot connector
    hubspot_config = ConnectorConfig(
        name="hubspot",
        connector_type="api",
        keyvault_secret="hubspot-api-key",
        landing_path="raw/crm/hubspot",
        extra={
            "base_url": "https://api.hubapi.com",
            "rate_limit": 10,
            "properties": {
                "contacts": ["firstname", "lastname", "email", "phone", "lifecyclestage"],
                "companies": ["name", "industry", "website", "country"],
                "deals": ["dealname", "amount", "dealstage", "closedate"],
            },
        },
    )

    # Create connector instance (API key would come from Key Vault in production)
    hubspot_api_key = "YOUR_HUBSPOT_API_KEY"  # Replace with actual key or use Key Vault
    connector = create_connector(hubspot_config, secret_value=hubspot_api_key)

    # Validate connection
    if not connector.validate_connection():
        print("Failed to connect to HubSpot")
        return

    print("✅ Connected to HubSpot API")

    # Example 1: Fetch contacts with default pagination
    print("\n--- Fetching Contacts ---")
    contacts = connector.extract(
        "/crm/v3/objects/contacts",
        params={
            "limit": 100,
            "properties": ["firstname", "lastname", "email", "phone", "lifecyclestage"],
        },
    )
    print(f"Fetched {len(contacts)} contacts")
    if contacts:
        print(f"Sample contact: {contacts[0]}")

    # Example 2: Fetch companies
    print("\n--- Fetching Companies ---")
    companies = connector.extract(
        "/crm/v3/objects/companies",
        params={
            "limit": 50,
            "properties": ["name", "industry", "website", "country", "annualrevenue"],
        },
    )
    print(f"Fetched {len(companies)} companies")
    if companies:
        print(f"Sample company: {companies[0]}")

    # Example 3: Fetch deals
    print("\n--- Fetching Deals ---")
    deals = connector.extract(
        "/crm/v3/objects/deals",
        params={
            "limit": 100,
            "properties": ["dealname", "amount", "dealstage", "closedate"],
        },
    )
    print(f"Fetched {len(deals)} deals")
    if deals:
        print(f"Sample deal: {deals[0]}")

    # Example 4: Get schema information
    print("\n--- Schema Information ---")
    schema = connector.get_schema()
    print(f"Available objects: {[obj['name'] for obj in schema.get('objects', [])]}")

    # Example 5: Get metadata
    print("\n--- Metadata ---")
    metadata = connector.get_metadata()
    print(f"Source: {metadata['_source_system']}")
    print(f"Type: {metadata['_connector_type']}")
    print(f"Landing path: {metadata['_landing_path']}")


if __name__ == "__main__":
    main()
