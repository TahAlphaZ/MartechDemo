# Google Sheets Connector

The Google Sheets connector enables extraction of data from Google Sheets via the Google Drive API. This connector is part of the MarTech Fabric Pipeline and integrates seamlessly with the medallion architecture for data ingestion.

## Features

- **Google Sheets API v4 Integration**: Direct access to Google Sheets data via REST API
- **Service Account Authentication**: Secure OAuth2 authentication using service account credentials
- **Flexible Range Notation**: Support for various Google Sheets range notations (e.g., `Sheet1!A:Z`, `Sheet1!A1:Z100`)
- **Automatic Schema Detection**: Detect column names and data types from sheet headers
- **Data Formatting**: Convert raw sheet data to dictionary format for pipeline processing
- **Metadata Attachment**: Standard metadata attached to all extractions for tracking and auditing
- **Error Handling**: Graceful handling of connection failures and API errors

## Configuration

### Basic Setup

1. **Enable Google Sheets API** on your Google Cloud project
2. **Create a service account** with Sheets API and Drive API permissions
3. **Store credentials** in Azure Key Vault with the configured secret name

### Configuration in `connector_registry.yaml`

```yaml
google_sheets:
  active: ["default"]  # Enable connectors by name
  connectors:
    default:
      type: "google_sheets_api"
      base_url: "https://sheets.googleapis.com/v4"
      auth_type: "service_account"
      keyvault_secret: "google-sheets-service-account-json"
      landing_path: "raw/sheets/default"
      # Optional configuration
      spreadsheet_id: "YOUR_SPREADSHEET_ID"  # Set for default spreadsheet
      sheet_name: "Sheet1"                    # Default sheet to extract
      value_render_option: "FORMATTED_VALUE"  # or UNFORMATTED_VALUE
      schema: {}  # Optional: predefined schema

    marketing_data:
      type: "google_sheets_api"
      base_url: "https://sheets.googleapis.com/v4"
      auth_type: "service_account"
      keyvault_secret: "google-sheets-marketing-account-json"
      spreadsheet_id: "1MARKETING_SHEETS_ID"
      sheet_name: "Sheet1"
      landing_path: "raw/sheets/marketing"
      rate_limit: 5  # requests per second
```

## Usage

### Extract Data from a Spreadsheet

```python
from src.connectors.connector_factory import load_registry, get_active_connectors, create_connector
from src.connectors.base_connector import ConnectorConfig

# Load configuration
registry = load_registry()
active_connectors = get_active_connectors(registry)

# Get service account credentials from Key Vault
service_account_json = "<service_account_json_from_key_vault>"

# Create connector
config = active_connectors["sheets_default"]
connector = create_connector(config, service_account_json)

# Validate connection
if connector.validate_connection():
    # Extract data from specific range
    data = connector.extract("Sheet1!A:Z", {
        "spreadsheet_id": "YOUR_SPREADSHEET_ID",
        "value_render_option": "FORMATTED_VALUE"
    })
    
    # Data is now a list of dictionaries
    for row in data:
        print(row)
else:
    print("Connection validation failed")
```

### Get Schema

```python
# Automatically detect schema from first row
schema = connector.get_schema()
print(schema)
# Output:
# {
#   "type": "object",
#   "properties": {
#     "column_name": {"type": "string"},
#     ...
#   },
#   "required": ["column_name", ...]
# }
```

### Extract with Parameters

```python
# Extract with custom parameters
data = connector.extract("Sheet1!A1:Z100", {
    "spreadsheet_id": "YOUR_SPREADSHEET_ID",
    "value_render_option": "UNFORMATTED_VALUE",
    "date_time_render_option": "SERIAL_NUMBER"
})
```

## Range Notation

The connector supports standard Google Sheets range notation:

- `Sheet1!A:Z` - All data in columns A through Z
- `Sheet1!A1:Z100` - Data from A1 to Z100
- `Sheet1!1:1` - Entire first row (useful for headers)
- `Sheet1!A:A` - Entire column A

## Service Account Credentials

The service account JSON file should look like:

```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "service-account@your-project-id.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

## Connector Type Detection

The factory automatically detects when to use `GoogleSheetsConnector` based on the connector name in the registry. The class is registered as:

```python
CONNECTOR_CLASS_MAP = {
    ...
    "google_sheets": GoogleSheetsConnector,
}
```

## Output Format

Data extracted from Google Sheets is automatically converted to a list of dictionaries:

```python
# Raw sheet data (what API returns):
[
  ["Name", "Email", "Age"],
  ["Alice", "alice@example.com", "28"],
  ["Bob", "bob@example.com", "35"]
]

# Formatted output (what extract() returns):
[
  {"Name": "Alice", "Email": "alice@example.com", "Age": "28"},
  {"Name": "Bob", "Email": "bob@example.com", "Age": "35"}
]
```

## Metadata

Each extraction includes standard metadata:

```python
metadata = connector.get_metadata()
# Returns:
# {
#   "_source_system": "google_sheets",
#   "_connector_type": "sheets",
#   "_landing_path": "raw/sheets/default",
#   "_sheets_api_version": "v4",
#   "_spreadsheet_id": "YOUR_SPREADSHEET_ID"
# }
```

## Error Handling

The connector gracefully handles common errors:

### Missing Spreadsheet ID
```python
# Will raise ValueError if spreadsheet_id not in params or config
try:
    data = connector.extract("Sheet1!A:Z")
except ValueError as e:
    print(f"Error: {e}")  # "spreadsheet_id required in params or config.extra"
```

### Connection Validation Failure
```python
# Returns False if connection cannot be established
if not connector.validate_connection():
    print("Failed to connect to Google Sheets API")
```

### Invalid Credentials
```python
# Raises ValueError if service account JSON is invalid
try:
    connector = GoogleSheetsConnector(config, "invalid json")
except ValueError as e:
    print(f"Error: {e}")  # "Invalid service account JSON format"
```

## Testing

The connector includes comprehensive unit tests:

```bash
# Run all Google Sheets connector tests
pytest tests/unit/connectors/test_google_sheets_connector.py -v

# Run specific test class
pytest tests/unit/connectors/test_google_sheets_connector.py::TestGoogleSheetsConnectorInitialization -v

# Run with coverage
pytest tests/unit/connectors/test_google_sheets_connector.py --cov=src.connectors.google_sheets_connector
```

### Test Coverage

- **Initialization**: Service account credential parsing and validation
- **Connection Validation**: API connectivity checks
- **Data Extraction**: Range notation support and data formatting
- **Schema Detection**: Header row analysis
- **Metadata**: Proper attachment of connector metadata
- **Factory Integration**: Correct instantiation via connector factory
- **Error Handling**: Graceful handling of failures

## Limitations

- Requires valid service account credentials stored in Azure Key Vault
- Google Sheets API rate limits apply (429 responses handled by retry logic)
- Large sheets (1M+ rows) may require pagination configuration
- Formulas are returned as formatted values by default

## Future Enhancements

- [ ] Support for OAuth2 user consent flow
- [ ] Incremental extraction based on modification timestamps
- [ ] Built-in retry logic with exponential backoff
- [ ] Support for multiple sheet extraction in single operation
- [ ] Google Drive folder scanning for multiple sheets
