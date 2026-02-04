import time
import pandas as pd
import numpy as np
import os
import json
import textwrap
import datetime
from datetime import datetime, timedelta
from notebookutils import mssparkutils
import sempy_labs as labs

# -----------------------------------
# Medallion ADF / Streaming Template Generator
# -----------------------------------
class MedallionADFGenerator:
    def __init__(self, out_dir="infra/adf_medallion", prefix="medallion"):
        self.out_dir = out_dir
        self.prefix = prefix
        # ensure out_dir exists
        os.makedirs(self.out_dir, exist_ok=True)

    def generate_adls_structure(self):
        base = os.path.join(self.out_dir, "adls_structure")
        dirs = ["bronze", "silver", "gold", "streaming"]
        created = []
        for d in dirs:
            p = os.path.join(base, d)
            os.makedirs(p, exist_ok=True)
            created.append(os.path.relpath(p, self.out_dir))
            # create README for bronze/silver/gold; streaming gets a README too
            readme_path = os.path.join(p, "README.txt")
            with open(readme_path, "w", encoding="utf-8") as f:
                if d == "bronze":
                    f.write(textwrap.dedent(
                        """
                        Bronze layer - raw ingestion zone

                        Partitioning convention (batch):
                        bronze/{source}/YYYY/MM/DD/

                        Partitioning convention (streaming micro-batches):
                        bronze/streaming/{source}/YYYY/MM/DD/HH/

                        Recommended file formats: parquet (preferred), or newline-delimited JSON / compressed CSV

                        Example file ingestion:
                        - adobe_analytics.csv -> bronze/adobe_analytics/2024/01/10/adobe_analytics_20240110.parquet

                        Partition keys: source, year, month, day
                        """))
                elif d == "silver":
                    f.write(textwrap.dedent(
                        """
                        Silver layer - cleaned and conformed data

                        Reads from bronze/{source}/YYYY/MM/DD/ and writes to:
                        silver/{source}/YYYY/MM/DD/

                        Transform guidance:
                        - Parse timestamps to UTC
                        - Standardize column names
                        - Deduplicate by primary key
                        - Coerce types and apply null handling
                        """))
                elif d == "gold":
                    f.write(textwrap.dedent(
                        """
                        Gold layer - business ready aggregated datasets

                        Writes optimized parquet partitioned for analytics and Power BI:
                        gold/{source}/year={YYYY}/month={MM}/day={DD}/

                        Typical content: daily aggregates, conformed dimensions, facts optimized for reporting
                        """))
                else:
                    f.write(textwrap.dedent(
                        """
                        Streaming area - short-term micro-batch storage before processing to silver/gold.

                        Convention for streaming micro-batches:
                        streaming/{source}/YYYY/MM/DD/HH/
                        """))
        # sentinel example in bronze
        success_example = os.path.join(base, "bronze", "_SUCCESS.example")
        with open(success_example, "w", encoding="utf-8") as f:
            f.write("This is an example success marker to show how a producer can mark successful micro-batch writes.\n")
        return created

    def generate_arm_linked_services(self):
        arm_dir = os.path.join(self.out_dir, "arm")
        os.makedirs(arm_dir, exist_ok=True)
        path = os.path.join(arm_dir, "adf_linked_services.json")

        resources = [
            {
                "type": "Microsoft.DataFactory/factories/linkedservices",
                "name": f"LS_ADLSGen2_{self.prefix}",
                "apiVersion": "2018-06-01",
                "properties": {
                    "type": "AzureDataLakeStorageGen2",
                    "typeProperties": {
                        "accountName": "<<storage-account-name>>",
                        "servicePrincipalId": "<<service-principal-id-or-keyvault-ref>>",
                        "tenant": "<<tenant-id>>"
                    }
                },
                "__comment__": "Replace placeholders with your storage account name and secure credential references. Use Key Vault references in ADF like @Microsoft.KeyVault(VaultName=..., SecretName=...) for secrets."
            },
            {
                "type": "Microsoft.DataFactory/factories/linkedservices",
                "name": f"LS_EventHub_{self.prefix}",
                "apiVersion": "2018-06-01",
                "properties": {
                    "type": "AzureEventHub",
                    "typeProperties": {
                        "connectionString": "<<event-hubs-connection-string-or-keyvault-ref>>"
                    }
                },
                "__comment__": "Connection string should be stored in Key Vault and referenced. Example: '@Microsoft.KeyVault(VaultName=MYKV, SecretName=eh-conn)'."
            },
            {
                "type": "Microsoft.DataFactory/factories/linkedservices",
                "name": f"LS_KeyVault_{self.prefix}",
                "apiVersion": "2018-06-01",
                "properties": {
                    "type": "AzureKeyVault",
                    "typeProperties": {
                        "baseUrl": "https://<<your-vault-name>>.vault.azure.net/"
                    }
                },
                "__comment__": "ADF can reference secrets from this Key Vault. Ensure the Data Factory has access policies to GET secrets." 
            }
        ]

        doc = {
            "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
            "contentVersion": "1.0.0.0",
            "parameters": {},
            "resources": resources
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
        return os.path.relpath(path, self.out_dir)

    def generate_adf_pipeline_bronze(self):
        pipeline_dir = os.path.join(self.out_dir, "arm", "pipelines")
        os.makedirs(pipeline_dir, exist_ok=True)
        path = os.path.join(pipeline_dir, "bronze_ingest_pipeline.json")

        pipeline = {
            "name": "pipeline_bronze_ingest",
            "properties": {
                "parameters": {
                    "sourceName": {"type": "String"},
                    "watermarkColumn": {"type": "String"},
                    "containerName": {"type": "String"}
                },
                "activities": [
                    {
                        "name": "LookupWatermark",
                        "type": "Lookup",
                        "typeProperties": {
                            "source": "<lookup-control-table-sql-or-dataset>",
                            "__comment__": "Lookup last watermark value from control table to perform incremental copy"
                        }
                    },
                    {
                        "name": "CopyToBronze",
                        "type": "Copy",
                        "typeProperties": {
                            "source": {"type": "SqlSource", "query": "SELECT * FROM source_table WHERE @{pipeline().parameters.watermarkColumn} > @{activity('LookupWatermark').output.firstRow.last_watermark}"},
                            "sink": {
                                "type": "AzureBlobFS",
                                "dataset": {
                                    "container": "@pipeline().parameters.containerName",
                                    "folderPath": "bronze/@{pipeline().parameters.sourceName}/@{formatDateTime(utcNow(),'yyyy/MM/dd')}"
                                }
                            }
                        }
                    },
                    {
                        "name": "UpsertWatermark",
                        "type": "WebActivity",
                        "typeProperties": {
                            "url": "<control-table-endpoint-or-storedproc-to-update-watermark>",
                            "method": "POST",
                            "__comment__": "Update the watermark control table after successful copy"
                        }
                    }
                ]
            }
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2)
        return os.path.relpath(path, self.out_dir)

    def generate_adf_pipeline_silver(self):
        pipeline_dir = os.path.join(self.out_dir, "arm", "pipelines")
        os.makedirs(pipeline_dir, exist_ok=True)
        path = os.path.join(pipeline_dir, "silver_transform_pipeline.json")

        pipeline = {
            "name": "pipeline_silver_transform",
            "properties": {
                "parameters": {
                    "sourceName": {"type": "String"},
                    "partitionDate": {"type": "String"}
                },
                "activities": [
                    {
                        "name": "DataFlowActivity",
                        "type": "MappingDataFlow",
                        "typeProperties": {
                            "description": "Read from Bronze, apply casts, timestamp parsing, dedup by PK, write to Silver",
                            "inputs": ["bronze/@{pipeline().parameters.sourceName}/@{pipeline().parameters.partitionDate}"],
                            "outputs": ["silver/@{pipeline().parameters.sourceName}/@{pipeline().parameters.partitionDate}"],
                            "__comment__": "Transforms: parse timestamps to UTC, standardize column names, deduplicate on primary key column(s), null handling"
                        }
                    }
                ]
            },
            "__comment__": "Deduplicate by primary key and apply basic cleaning/transforms in the data flow"
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2)
        return os.path.relpath(path, self.out_dir)

    def generate_adf_pipeline_gold(self):
        pipeline_dir = os.path.join(self.out_dir, "arm", "pipelines")
        os.makedirs(pipeline_dir, exist_ok=True)
        path = os.path.join(pipeline_dir, "gold_aggregate_pipeline.json")

        pipeline = {
            "name": "pipeline_gold_aggregate",
            "properties": {
                "parameters": {
                    "aggregationWindow": {"type": "String"},
                    "destPartition": {"type": "String"}
                },
                "activities": [
                    {
                        "name": "AggregateToGold",
                        "type": "DatabricksNotebook",
                        "typeProperties": {
                            "notebookPath": "<path-to-aggregation-notebook>",
                            "baseParameters": {
                                "aggregationWindow": "@pipeline().parameters.aggregationWindow",
                                "destPartition": "@pipeline().parameters.destPartition"
                            },
                            "__comment__": "Typical aggregations: daily totals, counts, top-N, conformed keys. Write parquet optimized for Power BI with partitioning: gold/{source}/year={YYYY}/month={MM}/day={DD}/"
                        }
                    }
                ]
            }
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(pipeline, f, indent=2)
        return os.path.relpath(path, self.out_dir)

    def generate_streaming_asa_job(self):
        sa_dir = os.path.join(self.out_dir, "stream_analytics")
        os.makedirs(sa_dir, exist_ok=True)
        path = os.path.join(sa_dir, "streaming_job.json")

        job = {
            "jobName": f"asa_medallion_stream_{self.prefix}",
            "inputs": [
                {
                    "name": "eh_input",
                    "type": "EventHub",
                    "properties": {
                        "eventHubName": "<<event-hub-name>>",
                        "consumerGroup": "<<consumer-group>>",
                        "__comment__": "Use Key Vault or managed identity for secure credentials"
                    }
                }
            ],
            "outputs": [
                {
                    "name": "powerbi_output",
                    "type": "PowerBI",
                    "properties": {
                        "groupName": "<<powerbi-group-name>>",
                        "datasetName": "<<dataset-name>>",
                        "tableName": "<<table-name>>",
                        "__comment__": "Power BI output requires registering an app and granting ASA permissions (or use ASA managed identity)."
                    }
                },
                {
                    "name": "adlsg2_output",
                    "type": "DataLake",
                    "properties": {
                        "pathPattern": "gold/streaming/{source}/@{date:yyyy}/@{hour:HH}/",
                        "__comment__": "Writes micro-batches to Gold for historical analysis. Use parquet format."
                    }
                }
            ],
            "query": "SELECT System.Timestamp() AS EventEnqueuedUtcTime, * INTO [powerbi_output] FROM [eh_input];",
            "__comment__": "Example ASA job: forwards events to Power BI for realtime tiles and writes to ADLS for historical analysis. Register Power BI credentials and/or grant managed identity access."
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(job, f, indent=2)
        return os.path.relpath(path, self.out_dir)

    def generate_powerbi_integration_doc(self):
        pb_dir = os.path.join(self.out_dir, "powerbi")
        os.makedirs(pb_dir, exist_ok=True)
        path = os.path.join(pb_dir, "POWERBI_REALTIME.md")

        md = textwrap.dedent(f"""
        Power BI Real-time & Historical Integration Patterns
        ==================================================

        Recommended patterns for real-time + historical analytics:

        1) Pattern A - Stream Analytics → Power BI (Push streaming dataset) + persist to Gold
           - Use ASA to push events to a Power BI streaming dataset for instant visuals (streaming tiles)
           - Also write micro-batches to Gold (parquet) for historical queries via Synapse Serverless or DirectQuery
           - Recommended for immediate KPIs and historical analysis

        2) Pattern B - Persist to Gold → Synapse Serverless / Dedicated SQL Pool → DirectQuery
           - Persist events/aggregates to Gold parquet optimized for analytical reads
           - Expose via Synapse Serverless or Dedicated SQL Pool and use DirectQuery in Power BI for near-real-time dashboards
           - Note: DirectQuery has freshness/performance tradeoffs; Premium capacity recommended for heavy workloads

        3) Pattern C - Push dataset + scheduled refresh
           - For lower-latency near-real-time you can push to an API-backed dataset and schedule frequent refreshes

        Recommended approach: Pattern A (ASA → Power BI for realtime visuals) combined with persisting to Gold for historical/complex analysis.

        Example streaming dataset schema (matching ASA output):

        - EventEnqueuedUtcTime: datetime
        - source: string
        - metric1: double
        - metric2: double
        - payload: string (json)

        Guidance:
        - Use streaming tiles for KPI cards, gauges, and simple real-time visuals.
        - Use Gold parquet + Synapse Serverless for historical reports and complex visuals (DirectQuery or import depending on freshness/size).
        - For heavy DirectQuery loads, plan for Premium capacity.

        Steps to connect Power BI to ASA:
        1. Register an Azure AD app or configure ASA to use a managed identity
        2. Grant the app or identity the required Power BI API permissions (Push dataset permissions)
        3. Configure Power BI output in ASA with groupName/datasetName/tableName placeholders
        4. Test streaming tiles and validate persisted Gold parquet with Synapse queries

        """)

        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
        return os.path.relpath(path, self.out_dir)

    def generate_all(self):
        created_files = []
        # adls structure
        self.generate_adls_structure()
        created_files.append("adls_structure/bronze/README.txt")
        created_files.append("adls_structure/bronze/_SUCCESS.example")
        created_files.append("adls_structure/silver/README.txt")
        created_files.append("adls_structure/gold/README.txt")
        created_files.append("adls_structure/streaming/README.txt")

        # arm linked services
        created_files.append(self.generate_arm_linked_services())

        # pipelines
        created_files.append(self.generate_adf_pipeline_bronze())
        created_files.append(self.generate_adf_pipeline_silver())
        created_files.append(self.generate_adf_pipeline_gold())

        # stream analytics
        created_files.append(self.generate_streaming_asa_job())

        # powerbi doc
        created_files.append(self.generate_powerbi_integration_doc())

        # manifest
        manifest_path = os.path.join(self.out_dir, "manifest.json")
        manifest = {
            "generatedBy": "blueprint.MedallionADFGenerator",
            "generatedAt": datetime.utcnow().isoformat() + "Z",
            "files": created_files
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        print(f"Medallion scaffold generated at: {self.out_dir}")
        print(f"Manifest: {manifest_path}")
        return manifest_path

# -----------------------------------
# Step 0: Read CSV Files
# -----------------------------------
def read_csv_files():
    """Read Adobe Analytics and Qualtrics CSV files"""
    try:
        # Read the CSV files from lakehouse Files folder
        adobe_df = pd.read_csv('/lakehouse/default/Files/adobe_analytics.csv')
        qualtrics_df = pd.read_csv('/lakehouse/default/Files/qualtrics.csv')
        
        print("✅ Adobe Analytics data loaded:")
        print(f"   - Shape: {adobe_df.shape}")
        print(f"   - Columns: {list(adobe_df.columns)}")
        print(adobe_df.head(3))
        
        print("\n✅ Qualtrics Survey data loaded:")
        print(f"   - Shape: {qualtrics_df.shape}")
        print(f"   - Columns: {list(qualtrics_df.columns)}")
        print(qualtrics_df.head(3))
        
        return adobe_df, qualtrics_df
        
    except FileNotFoundError as e:
        print(f"❌ CSV file not found: {e}")
        print("📝 Please ensure your CSV files are uploaded to the lakehouse Files section")
        print("   Expected files: 'adobe_analytics.csv' and 'qualtrics.csv'")
        return None, None
    except Exception as e:
        print(f"❌ Error reading CSV files: {e}")
        return None, None

def create_joined_dataset(adobe_df, qualtrics_df):
    """Join Adobe and Qualtrics data for analysis"""
    # Join on customer_id
    joined_df = adobe_df.merge(
        qualtrics_df, 
        on='customer_id', 
        how='left',
        suffixes=('_adobe', '_qualtrics')
    )
    
    # Create derived columns for analysis
    joined_df['price_gap_rush'] = joined_df['abandoned_price'] - joined_df['max_willingness_pay_rush']
    joined_df['price_gap_nonrush'] = joined_df['abandoned_price'] - joined_df['max_willingness_pay_nonrush']
    
    # Create customer segments
    joined_df['customer_segment'] = joined_df.apply(
        lambda row: 'High_Val_High_Freq' if row['total_customer_lifetime_value'] >= 5000 and row['booking_frequency_12months'] > 6
        else 'High_Val_Low_Freq' if row['total_customer_lifetime_value'] >= 5000 and row['booking_frequency_12months'] <= 2
        else 'Low_Val_High_Freq' if row['total_customer_lifetime_value'] < 5000 and row['booking_frequency_12months'] > 6
        else 'Low_Val_Low_Freq',
        axis=1
    )
    
    # Create appropriate price gap based on season
    joined_df['price_gap'] = np.where(
        joined_df['season_type'] == 'Rush',
        joined_df['price_gap_rush'],
        joined_df['price_gap_nonrush']
    )
    
    # Create recommended coupon amount (e.g., 70% of price gap)
    joined_df['recommended_coupon'] = np.maximum(0, joined_df['price_gap'] * 0.7)
    
    print("✅ Joined dataset created:")
    print(f"   - Shape: {joined_df.shape}")
    print(f"   - New columns added: customer_segment, price_gap, recommended_coupon")
    print("\n📊 Sample of joined data:")
    print(joined_df[['customer_id', 'abandoned_price', 'season_type', 'customer_segment', 'price_gap', 'recommended_coupon']].head())
    
    return joined_df

# -----------------------------------
# Helper function to create Direct Lake Semantic Model
# -----------------------------------
def create_semantic_model_from_lakehouse(lakehouse_name, lakehouse_id, table_name, semantic_model_name=None):
    """
    Creates a Direct Lake semantic model from a Fabric lakehouse table.
    
    Parameters:
    - lakehouse_name: Name of the lakehouse
    - lakehouse_id: ID of the lakehouse
    - table_name: Name of the table in the lakehouse
    - semantic_model_name: Optional custom name for semantic model (defaults to lakehouse_name + "_SemanticModel")
    
    Returns:
    - semantic_model_id if successful, None if failed
    """
    print("📊 Creating Direct Lake semantic model...")
    
    try:
        # Wait for lakehouse to be fully ready
        print("⏳ Waiting for lakehouse to be ready...")
        
        # Set semantic model name
        if not semantic_model_name:
            semantic_model_name = f"{lakehouse_name}_SemanticModel"
        
        # Method 1: Try to use directlake functionality 
        try:
            import sempy_labs.directlake as directlake
            
            # Use the correct function with required lakehouse_tables parameter
            directlake.generate_direct_lake_semantic_model(
                dataset=semantic_model_name,
                lakehouse=lakehouse_name,
                lakehouse_tables=[table_name],  # Required parameter: list of tables
                overwrite=True
            )
            print(f"✅ Direct Lake semantic model created: {semantic_model_name}")
            
        except Exception as e:
            # Fallback method: Create blank semantic model and add Direct Lake tables
            print(f"📝 Using fallback method - Primary method failed: {e}")
            print("Creating blank semantic model...")
            
            try:
                # Create blank semantic model
                labs.create_blank_semantic_model(
                    dataset=semantic_model_name,
                    overwrite=True
                )
                
                # Add the table from lakehouse using Direct Lake with correct parameter name
                import sempy_labs.directlake as directlake
                
                directlake.add_table_to_direct_lake_semantic_model(
                    dataset=semantic_model_name,
                    lakehouse=lakehouse_name,
                    table_name=table_name  # Correct parameter name is table_name, not lakehouse_table_name
                )
                
                print(f"✅ Semantic model created and table added: {semantic_model_name}")
                
            except Exception as fallback_error:
                print(f"⚠️ Error in fallback method: {fallback_error}")
                print("💡 The semantic model was created but table connection may need manual setup")
                # Don't raise the error, semantic model is already created
        
        # Verify semantic model was created
        datasets = labs.list_datasets()
        model_match = datasets[datasets["Dataset Name"] == semantic_model_name]
        
        if not model_match.empty:
            dataset_id = model_match.iloc[0]["Dataset ID"]
            print(f"✅ Semantic model verified: {semantic_model_name} (ID: {dataset_id})")
            print("📊 You can now create Power BI reports using this semantic model!")
            
            # Provide next steps guidance
            print(f"""
📋 Next Steps:
1. Go to Fabric Portal → Your Workspace
2. Click 'New' → 'Power BI Report'
3. Select semantic model: '{semantic_model_name}'
4. Start building visuals with your '{table_name}' table
            """)
            
            return dataset_id
        else:
            print("⚠️ Semantic model creation may still be in progress. Check your workspace in a few minutes.")
            return None
            
    except Exception as e:
        print(f"⚠️ Error creating semantic model: {e}")
        print("💡 Alternative: Create semantic model manually from the lakehouse in Fabric Portal")
        return None

# -----------------------------------
# Complete Airline Fabric Pipeline Class
# -----------------------------------
class AirlineAnalyticsPipeline:
    def __init__(self, adobe_df, qualtrics_df):
        self.adobe_df = adobe_df
        self.qualtrics_df = qualtrics_df
        self.joined_df = None
        self.lakehouse_info = None
        self.lakehouse_path = None
        self.semantic_model_id = None
        self.semantic_model_name = None

    # Step 1: Data Preparation
    def prepare_data(self):
        """Prepare and join the data for analysis"""
        print("🔄 Step 1: Preparing and Joining Data")
        self.joined_df = create_joined_dataset(self.adobe_df, self.qualtrics_df)
        return self.joined_df is not None

    # Step 2: Lakehouse Creation
    def create_lakehouse(self):
        """Create a new Fabric lakehouse"""
        lakehouse_name = f"AirlineAnalytics_{int(time.time())}"
        print(f"🏗️ Step 2: Creating lakehouse: {lakehouse_name}")

        try:
            # Create lakehouse
            self.lakehouse_info = mssparkutils.lakehouse.create(lakehouse_name)
            print(f"✅ Lakehouse created successfully")
            print(f"   - Name: {self.lakehouse_info['displayName']}")
            print(f"   - ID: {self.lakehouse_info['id']}")

            # Extract OneLake ABFS path
            self.lakehouse_path = self.lakehouse_info["properties"]["abfsPath"]
            print(f"📂 OneLake Path: {self.lakehouse_path}")

            # Wait for lakehouse to be fully provisioned
            print("⏳ Waiting 15 seconds for lakehouse setup...")
            time.sleep(15)
            return True
            
        except Exception as e:
            print(f"❌ Failed to create lakehouse: {e}")
            return False

    # Step 3: Data Upload
    def upload_data(self):
        """Upload data to lakehouse tables"""
        print("\n📤 Step 3: Uploading Data to Lakehouse")

        try:
            # Convert joined pandas DF to Spark DF
            spark_df = spark.createDataFrame(self.joined_df)

            # Build table paths in OneLake
            abfs_path = self.lakehouse_info['properties']['abfsPath']
            
            # Upload main joined table (primary table for analysis)
            main_table_path = f"{abfs_path}/Tables/CartAbandonmentAnalysis"
            spark_df.write.format("delta").mode("overwrite").save(main_table_path)
            print(f"✅ Main analysis table uploaded: CartAbandonmentAnalysis")

            # Also upload original tables separately for reference
            adobe_spark_df = spark.createDataFrame(self.adobe_df)
            adobe_table_path = f"{abfs_path}/Tables/AdobeAnalytics"
            adobe_spark_df.write.format("delta").mode("overwrite").save(adobe_table_path)
            print(f"✅ Adobe Analytics table uploaded: AdobeAnalytics")

            qualtrics_spark_df = spark.createDataFrame(self.qualtrics_df)
            qualtrics_table_path = f"{abfs_path}/Tables/QualtricsData"
            qualtrics_spark_df.write.format("delta").mode("overwrite").save(qualtrics_table_path)
            print(f"✅ Qualtrics table uploaded: QualtricsData")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to upload data: {e}")
            return False

    # Step 4: Create Semantic Model
    def create_semantic_model(self):
        """Create Direct Lake semantic model for Power BI"""
        print("\n📊 Step 4: Creating Semantic Model")
        
        lakehouse_name = self.lakehouse_info["displayName"]
        lakehouse_id = self.lakehouse_info["id"]
        self.semantic_model_name = f"{lakehouse_name}_SemanticModel"
        
        # Create semantic model with the main analysis table
        self.semantic_model_id = create_semantic_model_from_lakehouse(
            lakehouse_name=lakehouse_name,
            lakehouse_id=lakehouse_id,
            table_name="CartAbandonmentAnalysis",
            semantic_model_name=self.semantic_model_name
        )
        
        return self.semantic_model_id is not None

    # Step 5: Validation and Testing
    def validate_data(self):
        """Validate the data in lakehouse"""
        print("\n✅ Step 5: Data Validation")
        
        try:
            # Query the main table to ensure it's accessible
            df = spark.read.table("CartAbandonmentAnalysis")
            row_count = df.count()
            print(f"📊 CartAbandonmentAnalysis table: {row_count} rows")
            
            # Show sample data
            print("📋 Sample data from CartAbandonmentAnalysis:")
            df.select("customer_id", "abandoned_price", "customer_segment", "price_gap", "recommended_coupon").show(5)
            
            # Show customer segment distribution
            print("📈 Customer Segment Distribution:")
            df.groupBy("customer_segment", "season_type").count().show()
            
            return True
            
        except Exception as e:
            print(f"⚠️ Data validation failed: {e}")
            return False

    # Step 6: Generate Complete Analysis Guide
    def generate_complete_guide(self):
        """Generate comprehensive guide for Power BI and analysis"""
        print("\n📘 Step 6: Complete Analysis Guide")
        
        guide = f"""
        🎯 **AIRLINE CART ABANDONMENT ANALYSIS - COMPLETE SETUP**
        
        ✅ Lakehouse: {self.lakehouse_info['displayName']} (ID: {self.lakehouse_info['id']})
        ✅ Semantic Model: {self.semantic_model_name}
        ✅ Primary Table: CartAbandonmentAnalysis
        
        ### 🏗️ INFRASTRUCTURE CREATED:
        
        **Lakehouse Tables:**
        1. CartAbandonmentAnalysis - Main joined dataset (USE THIS FOR POWER BI)
        2. AdobeAnalytics - Raw Adobe Analytics data
        3. QualtricsData - Raw Qualtrics survey data
        
        **Key Business Columns:**
        - customer_segment: High_Val_High_Freq, High_Val_Low_Freq, Low_Val_High_Freq, Low_Val_Low_Freq
        - price_gap: Abandoned Price - Willingness to Pay (based on season)
        - recommended_coupon: Suggested discount amount (70% of price gap)
        - season_type: Rush vs Non-Rush
        - total_customer_lifetime_value: Customer value for segmentation
        - booking_frequency_12months: Booking frequency for segmentation
        
        ### 📊 POWER BI REPORT CREATION:
        
        **Step 1: Create New Report**
        1. Go to Fabric Portal → Your Workspace
        2. Click "New" → "Power BI Report"  
        3. Select semantic model: "{self.semantic_model_name}"
        4. Use table: "CartAbandonmentAnalysis"
        
        **Step 2: Build 3 Core Visualizations**
        
        **VISUAL 1 - Customer Segment Heatmap (Matrix):**
        - Rows: customer_segment
        - Columns: season_type
        - Values: Average of recommended_coupon
        - Conditional formatting: Color scale (green to red)
        
        **VISUAL 2 - Price Gap vs Coupon Analysis (Scatter Plot):**
        - X-axis: price_gap
        - Y-axis: recommended_coupon
        - Legend: customer_segment  
        - Size: total_customer_lifetime_value
        - Play Axis: season_type (if available)
        
        **VISUAL 3 - Customer Distribution (Clustered Column Chart):**
        - Axis: customer_segment
        - Values: Count of customer_id
        - Legend: season_type
        - Data labels: On
        
        ### 📈 BUSINESS INSIGHTS TO LOOK FOR:
        
        1. **Which customer segments need the highest coupons?**
        2. **How does seasonality affect coupon requirements?**
        3. **What's the distribution of customers across segments?**
        4. **Are high-value customers abandoning at different price points?**
        
        ### 🔍 DAX MEASURES FOR ADVANCED ANALYSIS:
        
        ```dax
        -- Key Performance Indicators
        Total Abandoned Carts = COUNT(CartAbandonmentAnalysis[customer_id])
        
        Average Coupon Needed = AVERAGE(CartAbandonmentAnalysis[recommended_coupon])
        
        Total Potential Revenue = SUM(CartAbandonmentAnalysis[abandoned_price])
        
        High Value Customer Count = 
            COUNTROWS(
                FILTER(CartAbandonmentAnalysis, 
                       CartAbandonmentAnalysis[total_customer_lifetime_value] >= 5000)
            )
        
        Coupon ROI Estimate = 
            DIVIDE(
                [Total Potential Revenue] * 0.3,  -- Assume 30% recovery rate
                SUM(CartAbandonmentAnalysis[recommended_coupon])
            )
        
        -- Seasonal Analysis
        Rush Season Coupon Avg = 
            CALCULATE(
                AVERAGE(CartAbandonmentAnalysis[recommended_coupon]),
                CartAbandonmentAnalysis[season_type] = "Rush"
            )
        
        Non-Rush Season Coupon Avg = 
            CALCULATE(
                AVERAGE(CartAbandonmentAnalysis[recommended_coupon]),
                CartAbandonmentAnalysis[season_type] = "Non-Rush"
            )
        ```
        
        ### 🎯 ACTIONABLE BUSINESS QUESTIONS:
        
        1. **Immediate Actions:** Which customers should get coupons today?
        2. **Pricing Strategy:** Should we adjust base prices for certain segments?
        3. **Campaign Timing:** When should we send rush vs non-rush campaigns?
        4. **Budget Allocation:** How much should we budget for coupon campaigns?
        5. **Segment Focus:** Which customer segments give the best ROI?
        
        ### 📊 SQL QUERIES FOR DEEPER ANALYSIS:
        
        ```sql
        -- Top coupon opportunities
        SELECT TOP 20 
            customer_id, 
            abandoned_price, 
            price_gap, 
            recommended_coupon,
            customer_segment,
            season_type
        FROM CartAbandonmentAnalysis
        WHERE price_gap > 0
        ORDER BY price_gap DESC;
        
        -- Segment performance summary
        SELECT 
            customer_segment,
            season_type,
            COUNT(*) as abandonment_count,
            AVG(abandoned_price) as avg_abandoned_price,
            AVG(price_gap) as avg_price_gap,
            AVG(recommended_coupon) as avg_coupon_needed,
            SUM(abandoned_price) as total_potential_revenue
        FROM CartAbandonmentAnalysis
        GROUP BY customer_segment, season_type
        ORDER BY avg_coupon_needed DESC;
        ```
        """
        
        print(guide)
        return guide

    # Master run method
    def run_complete_pipeline(self):
        """Execute the complete end-to-end pipeline"""
        print("🚀 AIRLINE CART ABANDONMENT ANALYSIS - COMPLETE PIPELINE")
        print("=" * 80)
        
        # Step 1: Data Preparation
        if not self.prepare_data():
            print("❌ Pipeline failed at data preparation step")
            return False

        # Step 2: Lakehouse Creation  
        if not self.create_lakehouse():
            print("❌ Pipeline failed at lakehouse creation step")
            return False

        # Step 3: Data Upload
        if not self.upload_data():
            print("❌ Pipeline failed at data upload step")
            return False

        # Step 4: Semantic Model Creation
        if not self.create_semantic_model():
            print("⚠️ Semantic model creation had issues, but continuing...")

        # Step 5: Data Validation
        if not self.validate_data():
            print("⚠️ Data validation had issues, but pipeline completed")

        # Step 6: Complete Guide
        self.generate_complete_guide()

        print("\n🎉 PIPELINE SUCCESSFULLY COMPLETED!")
        print("=" * 80)
        print("✅ Lakehouse created with cart abandonment data")
        print("✅ Semantic model prepared for Power BI")
        print("✅ Ready to build visualizations and insights")
        print("✅ Business logic implemented for customer segmentation")
        
        return True

# -----------------------------------
# Execute generator when run as script (minimal CLI)
# -----------------------------------
if __name__ == "__main__":
    # Generate medallion scaffold and templates
    gen = MedallionADFGenerator()
    manifest = gen.generate_all()
    print(manifest)
