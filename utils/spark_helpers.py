"""
Utility functions for Spark DataFrames and general data processing
"""

def spark_session_helper():
    """
    Create and configure a Spark session with optimal settings
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("DataFramesTutorial") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return spark

def display_dataframe_info(df):
    """
    Display comprehensive information about a DataFrame
    """
    print("DataFrame Schema:")
    df.printSchema()
    print(f"\nRow count: {df.count()}")
    print(f"Column count: {len(df.columns)}")
    print("\nFirst 5 rows:")
    df.show(5)

def create_sample_data():
    """
    Create sample datasets for learning purposes
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    # Customer data
    customers = pd.DataFrame({
        'customer_id': range(1, 101),
        'name': [f'Customer_{i}' for i in range(1, 101)],
        'age': np.random.randint(18, 80, 100),
        'city': np.random.choice(['New York', 'London', 'Tokyo', 'Paris', 'Sydney'], 100),
        'signup_date': [datetime.now() - timedelta(days=np.random.randint(1, 365)) for _ in range(100)]
    })
    
    # Sales data
    sales = pd.DataFrame({
        'sale_id': range(1, 501),
        'customer_id': np.random.randint(1, 101, 500),
        'product': np.random.choice(['Laptop', 'Phone', 'Tablet', 'Watch', 'Headphones'], 500),
        'amount': np.random.uniform(50, 2000, 500).round(2),
        'sale_date': [datetime.now() - timedelta(days=np.random.randint(1, 90)) for _ in range(500)]
    })
    
    return customers, sales

if __name__ == "__main__":
    print("Utility functions loaded successfully!")
