"""
Utility functions for Spark DataFrames tutorials
Helper functions to support the learning notebooks in this repository
"""

def create_spark_session(app_name="DataFramesTutorial"):
    """
    Create and configure a Spark session for local development
    Note: In Databricks, spark session is automatically available
    """
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        return spark
    except ImportError:
        print("PySpark not available. Install with: pip install pyspark")
        return None

def display_dataframe_info(df):
    """
    Display comprehensive information about a DataFrame
    Compatible with both Databricks and local environments
    """
    print("DataFrame Schema:")
    df.printSchema()
    print(f"\nRow count: {df.count()}")
    print(f"Column count: {len(df.columns)}")
    print(f"Columns: {df.columns}")
    print("\nFirst 5 rows:")
    df.show(5)

def create_sample_people_data():
    """
    Create the sample people dataset used in notebook 01
    Returns the data structure that can be converted to a DataFrame
    """
    return [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "Chicago"},
        {"name": "Charlie", "age": 35, "city": "San Francisco"}
    ]

def create_sample_cities_data():
    """
    Create the sample cities dataset used in notebook 02
    Returns the data structure for join operations
    """
    return [
        {"city": "New York", "state": "NY"},
        {"city": "Chicago", "state": "IL"},
        {"city": "San Francisco", "state": "CA"},
        {"city": "Los Angeles", "state": "CA"}
    ]

def create_messy_data_sample():
    """
    Create the messy dataset used in notebook 03 for data quality examples
    Returns data with various quality issues for cleaning practice
    """
    return [
        {"name": "Alice Johnson", "age": 30, "city": "New York", "email": "alice@company.com"},
        {"name": "Bob Smith", "age": None, "city": "Chicago", "email": "bob@test.com"},
        {"name": "Charlie Brown", "age": 35, "city": None, "email": "charlie.brown@email.org"},
        {"name": "  Diana Ross  ", "age": 28, "city": "  Los Angeles  ", "email": "diana@music.com"},
        {"name": "Eve", "age": 25, "city": "Miami", "email": "invalid-email"},
        {"name": "Frank Miller", "age": 150, "city": "Seattle", "email": "frank@old.com"},
        {"name": "", "age": 29, "city": "Denver", "email": "mystery@unknown.com"},
    ]

def validate_dataframe_join(left_df, right_df, join_key):
    """
    Validate DataFrames before joining to avoid common issues
    """
    print(f"Left DataFrame - Rows: {left_df.count()}, Columns: {len(left_df.columns)}")
    print(f"Right DataFrame - Rows: {right_df.count()}, Columns: {len(right_df.columns)}")
    
    if join_key not in left_df.columns:
        print(f"WARNING: Join key '{join_key}' not found in left DataFrame")
    if join_key not in right_df.columns:
        print(f"WARNING: Join key '{join_key}' not found in right DataFrame")
        
    print(f"Join key '{join_key}' is valid for both DataFrames")

def quick_data_quality_check(df):
    """
    Perform a quick data quality assessment
    """
    from pyspark.sql.functions import col, isnan, when, count
    
    print("=== Data Quality Summary ===")
    print(f"Total Rows: {df.count()}")
    print(f"Total Columns: {len(df.columns)}")
    
    # Check for nulls in each column
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    
    print("\nNull counts by column:")
    for col_name in df.columns:
        null_count = null_counts[col_name]
        if null_count > 0:
            print(f"  {col_name}: {null_count} nulls")
    
    print("\nFirst 3 rows:")
    df.show(3)

if __name__ == "__main__":
    print("Spark DataFrames utility functions loaded successfully!")
    print("Available functions:")
    print("  - create_spark_session()")
    print("  - display_dataframe_info()")
    print("  - create_sample_people_data()")
    print("  - create_sample_cities_data()")
    print("  - create_messy_data_sample()")
    print("  - validate_dataframe_join()")
    print("  - quick_data_quality_check()")
