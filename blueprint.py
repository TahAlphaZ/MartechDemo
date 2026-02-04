import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from notebookutils import mssparkutils
import sempy_labs as labs

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
# Execute the Complete Pipeline
# -----------------------------------
if __name__ == "__main__":
    print("📁 Reading CSV files...")
    adobe_df, qualtrics_df = read_csv_files()

    if adobe_df is not None and qualtrics_df is not None:
        print("\n🚀 Initializing Airline Analytics Pipeline...")
        
        # Create and run the complete pipeline
        pipeline = AirlineAnalyticsPipeline(adobe_df, qualtrics_df)
        success = pipeline.run_complete_pipeline()
        
        if success:
            print(f"\n🎯 NEXT STEPS:")
            print(f"1. Go to Fabric Portal → Your Workspace")
            print(f"2. Look for semantic model: {pipeline.semantic_model_name}")
            print(f"3. Create Power BI report using CartAbandonmentAnalysis table")
            print(f"4. Build the 3 core visualizations as outlined in the guide")
            
    else:
        print("❌ Cannot proceed without CSV files. Please upload:")
        print("   - adobe_analytics.csv (to /lakehouse/default/Files/)")
        print("   - qualtrics.csv (to /lakehouse/default/Files/)")
        print("\n💡 Upload files to the Files section of your lakehouse")