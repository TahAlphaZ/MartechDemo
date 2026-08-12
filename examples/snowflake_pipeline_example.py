"""
Snowflake Connector Integration Example
Shows how to use the Snowflake connector in a MarTech pipeline workflow.
"""
import logging
from typing import Dict, Any, List
from src.connectors.connector_factory import (
    load_registry,
    get_active_connectors,
    create_connector,
)
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakePipelineExample:
    """
    Example workflow demonstrating Snowflake connector usage in the MarTech pipeline.
    
    This shows:
    1. Loading connector registry
    2. Fetching credentials from Azure Key Vault
    3. Creating Snowflake connector instance
    4. Extracting marketing analytics data
    5. Validating data quality
    """

    def __init__(self, keyvault_url: str):
        """
        Initialize pipeline with Key Vault credentials.
        
        Args:
            keyvault_url: Azure Key Vault URL (e.g., https://myvault.vault.azure.net/)
        """
        self.keyvault_url = keyvault_url
        self.credential = DefaultAzureCredential()
        self.keyvault_client = SecretClient(
            vault_url=keyvault_url,
            credential=self.credential,
        )

    def get_secret(self, secret_name: str) -> str:
        """Retrieve secret from Key Vault."""
        try:
            secret = self.keyvault_client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_name}: {e}")
            raise

    def extract_campaigns(self) -> List[Dict[str, Any]]:
        """
        Extract campaigns from Snowflake.
        
        Returns:
            List of campaign records with ID, NAME, STATUS, BUDGET, etc.
        """
        # Load registry configuration
        registry = load_registry()
        active_connectors = get_active_connectors(registry)

        # Get Snowflake config (only works if active: snowflake in registry)
        if "db_snowflake" not in active_connectors:
            logger.error("Snowflake connector not active. Check connector_registry.yaml")
            return []

        sf_config = active_connectors["db_snowflake"]

        # Get connection parameters from Key Vault
        connection_params = self.get_secret(sf_config.keyvault_secret)

        # Create Snowflake connector
        connector = create_connector(sf_config, connection_params)

        # Validate connection before extraction
        if not connector.validate_connection():
            raise Exception("Failed to connect to Snowflake")

        # Extract campaigns
        logger.info("Extracting campaigns from Snowflake...")
        campaigns = connector.extract(
            """
            SELECT 
                ID,
                NAME,
                STATUS,
                BUDGET,
                START_DATE,
                END_DATE,
                CREATED_AT,
                UPDATED_AT
            FROM MARKETING.CAMPAIGNS
            ORDER BY CREATED_AT DESC
            """
        )

        logger.info(f"Extracted {len(campaigns)} campaigns")
        return campaigns

    def extract_customers(self) -> List[Dict[str, Any]]:
        """
        Extract customer data from Snowflake.
        
        Returns:
            List of customer records with ID, EMAIL, SEGMENT, LIFECYCLE_STAGE, etc.
        """
        registry = load_registry()
        active_connectors = get_active_connectors(registry)
        sf_config = active_connectors.get("db_snowflake")

        if not sf_config:
            raise Exception("Snowflake connector not configured")

        connection_params = self.get_secret(sf_config.keyvault_secret)
        connector = create_connector(sf_config, connection_params)

        if not connector.validate_connection():
            raise Exception("Snowflake connection failed")

        logger.info("Extracting customers from Snowflake...")
        customers = connector.extract(
            """
            SELECT 
                ID,
                EMAIL,
                SEGMENT,
                LIFECYCLE_STAGE,
                ACQUISITION_DATE,
                LTV_USD,
                CREATED_AT
            FROM MARKETING.CUSTOMERS
            WHERE ACTIVE = true
            """
        )

        logger.info(f"Extracted {len(customers)} active customers")
        return customers

    def extract_orders(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Extract recent orders from Snowflake.
        
        Args:
            days_back: Extract orders from last N days
            
        Returns:
            List of order records with ID, CUSTOMER_ID, TOTAL_AMOUNT, etc.
        """
        registry = load_registry()
        active_connectors = get_active_connectors(registry)
        sf_config = active_connectors.get("db_snowflake")

        connection_params = self.get_secret(sf_config.keyvault_secret)
        connector = create_connector(sf_config, connection_params)

        if not connector.validate_connection():
            raise Exception("Snowflake connection failed")

        logger.info(f"Extracting orders from last {days_back} days...")
        orders = connector.extract(
            f"""
            SELECT 
                ID,
                CUSTOMER_ID,
                TOTAL_AMOUNT,
                ORDER_DATE,
                CAMPAIGN_ID,
                CREATED_AT
            FROM MARKETING.ORDERS
            WHERE ORDER_DATE >= DATEADD(day, -{days_back}, CURRENT_DATE)
            ORDER BY ORDER_DATE DESC
            """
        )

        logger.info(f"Extracted {len(orders)} orders")
        return orders

    def profile_campaigns_table(self) -> Dict[str, Any]:
        """
        Profile the campaigns table for data quality validation.
        
        Returns:
            Dictionary with table statistics (row count, columns, sample data)
        """
        registry = load_registry()
        active_connectors = get_active_connectors(registry)
        sf_config = active_connectors.get("db_snowflake")

        connection_params = self.get_secret(sf_config.keyvault_secret)
        connector = create_connector(sf_config, connection_params)

        logger.info("Profiling CAMPAIGNS table...")

        # Get row count
        row_count = connector.get_table_count("MARKETING.CAMPAIGNS")

        # Get sample data
        sample = connector.get_table_sample("MARKETING.CAMPAIGNS", limit=5)

        # Get schema
        schema = connector.get_schema()

        profile = {
            "table_name": "MARKETING.CAMPAIGNS",
            "row_count": row_count,
            "sample_data": sample,
            "columns": [
                {
                    "name": col["COLUMN_NAME"],
                    "type": col["DATA_TYPE"],
                    "nullable": col["IS_NULLABLE"],
                }
                for col in schema
                if col["TABLE_NAME"] == "CAMPAIGNS"
            ],
        }

        logger.info(f"Profile complete: {row_count} rows in CAMPAIGNS table")
        return profile

    def run_full_extraction(self) -> Dict[str, Any]:
        """
        Run complete data extraction pipeline.
        
        Returns:
            Dictionary with extracted data from all tables
        """
        logger.info("Starting MarTech pipeline extraction from Snowflake...")

        try:
            extraction_result = {
                "campaigns": self.extract_campaigns(),
                "customers": self.extract_customers(),
                "orders": self.extract_orders(days_back=30),
                "table_profile": self.profile_campaigns_table(),
            }

            logger.info("✓ Data extraction completed successfully")
            return extraction_result

        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            raise


# Example usage
if __name__ == "__main__":
    # Initialize pipeline with your Key Vault
    keyvault_url = "https://your-keyvault-name.vault.azure.net/"
    pipeline = SnowflakePipelineExample(keyvault_url)

    try:
        # Extract campaigns
        campaigns = pipeline.extract_campaigns()
        print(f"\nExtracted {len(campaigns)} campaigns")

        # Extract customers
        customers = pipeline.extract_customers()
        print(f"Extracted {len(customers)} customers")

        # Extract recent orders
        orders = pipeline.extract_orders(days_back=7)
        print(f"Extracted {len(orders)} orders from last 7 days")

        # Profile table
        profile = pipeline.profile_campaigns_table()
        print(f"\nCampaigns table has {profile['row_count']} rows")
        print(f"Columns: {[col['name'] for col in profile['columns']]}")

    except Exception as e:
        print(f"Pipeline failed: {e}")
