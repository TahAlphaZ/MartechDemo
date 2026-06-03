"""
Microsoft Access Connector - Usage Examples
==============================================

This module demonstrates how to use the MicrosoftAccessConnector
to extract marketing data from Access databases.

Prerequisites:
    - Install dependencies: pip install -e ".[access]"
    - Have an Access database (.mdb or .accdb) available
    - Configure Azure Key Vault with database path (production)
"""

from typing import List, Dict, Any
from src.connectors.access_connector import MicrosoftAccessConnector
from src.connectors.base_connector import ConnectorConfig


# =============================================================================
# EXAMPLE 1: Basic Connection and Query
# =============================================================================

def example_basic_query():
    """Connect to Access database and execute a simple query."""
    
    config = ConnectorConfig(
        name="access",
        connector_type="database",
        keyvault_secret="access-db-path",
        landing_path="raw/database/access",
        extra={"readonly": True}
    )
    
    db_path = r"C:\MarketingData\CRM.accdb"
    connector = MicrosoftAccessConnector(config, db_path)
    
    results = connector.execute("SELECT TOP 10 * FROM Customers")
    
    print(f"Retrieved {len(results)} customers:")
    for row in results:
        print(f"  - {row.get('CustomerName', 'N/A')}")


# =============================================================================
# EXAMPLE 2: Parameterized Query with Date Filtering
# =============================================================================

def example_parameterized_query():
    """Use parameters to safely filter results by date range."""
    
    config = ConnectorConfig(
        name="access",
        connector_type="database",
        keyvault_secret="access-db-path",
        landing_path="raw/database/access",
        extra={"readonly": True}
    )
    
    db_path = r"C:\MarketingData\CRM.accdb"
    connector = MicrosoftAccessConnector(config, db_path)
    
    query = """
        SELECT 
            OrderID,
            CustomerID,
            OrderDate,
            OrderTotal
        FROM Orders
        WHERE OrderDate >= ?
        AND OrderTotal > ?
        ORDER BY OrderDate DESC
    """
    
    params = {"start_date": "2024-01-01", "min_total": 100.00}
    results = connector.execute(query, params)
    
    print(f"\nFound {len(results)} high-value orders since 2024:")
    total_revenue = sum(row.get('OrderTotal', 0) for row in results)
    print(f"Total Revenue: ${total_revenue:,.2f}")


# =============================================================================
# EXAMPLE 3: Schema Discovery
# =============================================================================

def example_schema_discovery():
    """Discover all tables and columns in the Access database."""
    
    config = ConnectorConfig(
        name="access",
        connector_type="database",
        keyvault_secret="access-db-path",
        landing_path="raw/database/access",
        extra={"readonly": True, "include_system_tables": False}
    )
    
    db_path = r"C:\MarketingData\CRM.accdb"
    connector = MicrosoftAccessConnector(config, db_path)
    
    schema = connector.get_schema()
    
    print("\n=== Database Schema ===")
    for table in schema["tables"]:
        print(f"\nTable: {table['table_name']}")
        print(f"Columns: {len(table['columns'])}")
        for col in table["columns"]:
            nullable = "NULL" if col.get("nullable") else "NOT NULL"
            print(f"  - {col['column_name']:30s} {col['data_type']:15s} {nullable}")


# =============================================================================
# EXAMPLE 4: Context Manager Pattern
# =============================================================================

def example_context_manager():
    """Use context manager for automatic connection cleanup."""
    
    config = ConnectorConfig(
        name="access",
        connector_type="database",
        keyvault_secret="access-db-path",
        landing_path="raw/database/access",
        extra={"readonly": True}
    )
    
    db_path = r"C:\MarketingData\CRM.accdb"
    
    with MicrosoftAccessConnector(config, db_path) as connector:
        customers = connector.execute("SELECT COUNT(*) AS total FROM Customers")
        orders = connector.execute("SELECT COUNT(*) AS total FROM Orders")
        
        print(f"\nDatabase Statistics:")
        print(f"  Total Customers: {customers[0]['total']}")
        print(f"  Total Orders: {orders[0]['total']}")


# =============================================================================
# EXAMPLE 5: Marketing Campaign Analysis
# =============================================================================

def example_campaign_analysis():
    """Analyze marketing campaign performance from Access database."""
    
    config = ConnectorConfig(
        name="access",
        connector_type="database",
        keyvault_secret="access-db-path",
        landing_path="raw/database/access",
        extra={"readonly": True}
    )
    
    db_path = r"C:\MarketingData\CRM.accdb"
    connector = MicrosoftAccessConnector(config, db_path)
    
    query = """
        SELECT 
            c.CampaignName,
            c.Budget,
            COUNT(DISTINCT o.OrderID) AS OrderCount,
            COUNT(DISTINCT o.CustomerID) AS UniqueCustomers,
            SUM(o.OrderTotal) AS TotalRevenue,
            (SUM(o.OrderTotal) - c.Budget) / c.Budget * 100 AS ROI_Percent
        FROM Campaigns c
        LEFT JOIN Orders o ON o.CampaignID = c.CampaignID
        WHERE c.StartDate >= ?
        GROUP BY c.CampaignID, c.CampaignName, c.Budget
        ORDER BY ROI_Percent DESC
    """
    
    results = connector.execute(query, {"start_date": "2024-01-01"})
    
    print("\n=== Campaign Performance Report ===")
    for campaign in results:
        print(f"\nCampaign: {campaign['CampaignName']}")
        print(f"  Budget: ${campaign['Budget']:,.2f}")
        print(f"  Revenue: ${campaign.get('TotalRevenue', 0):,.2f}")
        print(f"  ROI: {campaign.get('ROI_Percent', 0):.1f}%")


if __name__ == "__main__":
    print("Microsoft Access Connector - Usage Examples")
    print("=" * 50)
    print("\nUncomment examples in the code to run them.")
