# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality and Cleaning with PySpark
# MAGIC
# MAGIC This notebook demonstrates common data quality and cleaning techniques using PySpark DataFrames. It covers handling nulls, validating data types, cleaning strings, and detecting/removing duplicates. Example pandas code is also included for comparison.
# MAGIC
# MAGIC ## 1. Handling Nulls
# MAGIC
# MAGIC ### PySpark Example

# COMMAND ----------

# Enhanced dataset with various data quality issues
# Each dictionary represents a row with potential data quality problems
# Includes missing values, invalid formats, duplicates, and edge cases

data = [
    {"name": "Alice Johnson", "age": 30, "city": "New York", "email": "alice@company.com", "phone": "123-456-7890", "salary": 75000, "hire_date": "2020-01-15"},
    {"name": "Bob Smith", "age": None, "city": "Chicago", "email": "bob@test.com", "phone": "555.123.4567", "salary": 65000, "hire_date": "2019-03-22"},
    {"name": "Charlie Brown", "age": 35, "city": None, "email": "charlie.brown@email.org", "phone": "(555) 123-4567", "salary": 80000, "hire_date": "2021-07-10"},
    {"name": "  Diana Ross  ", "age": 28, "city": "  Los Angeles  ", "email": "diana@music.com", "phone": "555-987-6543", "salary": 70000, "hire_date": "2022-02-01"},
    {"name": "Eve", "age": 25, "city": "Miami", "email": "invalid-email", "phone": "123", "salary": 60000, "hire_date": "2023-01-01"},
    {"name": "Frank Miller", "age": 150, "city": "Seattle", "email": "frank@old.com", "phone": "999-888-7777", "salary": -5000, "hire_date": "1990-12-25"},
    {"name": "Grace   O'Connor", "age": 32, "city": "Boston", "email": "grace@company.com", "phone": "617-555-0123", "salary": 85000, "hire_date": "2020-06-15"},
    {"name": "", "age": 29, "city": "Denver", "email": "mystery@unknown.com", "phone": "303-555-1234", "salary": 72000, "hire_date": "2021-11-30"},
    {"name": "Henry Wilson", "age": -5, "city": "Portland", "email": "henry@test.com", "phone": "503.555.9876", "salary": 78000, "hire_date": "2022-08-14"},
    {"name": "Ivy Chen", "age": 26, "city": "San Francisco", "email": "ivy.chen@tech.com", "phone": "+1-415-555-0199", "salary": 95000, "hire_date": "2023-03-01"},
    # Duplicate entries
    {"name": "Alice Johnson", "age": 30, "city": "New York", "email": "alice@company.com", "phone": "123-456-7890", "salary": 75000, "hire_date": "2020-01-15"},
    {"name": "Jack Brown", "age": 40, "city": "Chicago", "email": "jack@company.com", "phone": "312-555-0001", "salary": 90000, "hire_date": "2018-05-20"},
    # Missing data
    {"name": "Karen Davis", "age": None, "city": None, "email": None, "phone": None, "salary": None, "hire_date": None},
    {"name": "Leo Martinez", "age": 35, "city": "Austin", "email": "leo@startup.io", "phone": "512-555-7890", "salary": 88000, "hire_date": "2019-09-12"},
    {"name": "123 Invalid Name", "age": 30, "city": "Phoenix", "email": "numbers@test.com", "phone": "602-555-3456", "salary": 67000, "hire_date": "2021-04-18"},
    # Edge cases
    {"name": "Mia Long-Hyphenated-Name", "age": 24, "city": "Nashville", "email": "mia@longname.com", "phone": "615-555-2468", "salary": 55000, "hire_date": "2023-06-01"},
    {"name": "Nick Rodriguez", "age": 45, "city": "Las Vegas", "email": "nick@casino.com", "phone": "702-555-1357", "salary": 120000, "hire_date": "2017-10-03"},
    {"name": "Olivia Taylor", "age": 17, "city": "Las Vegas", "email": "underage@test.com", "phone": "702-555-9999", "salary": 30000, "hire_date": "2024-01-01"}  # Business rule violation
]

df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

# Load and display sample data

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a Spark session
spark = SparkSession.builder \
    .appName("Sample Data") \
    .getOrCreate()

# Create a sample dataset with various data quality issues
data = [
    Row(id=1, name="Alice", age=25, salary=None),
    Row(id=2, name=None, age=30, salary=50000),
    Row(id=3, name="Bob", age=None, salary=60000),
    Row(id=4, name="Charlie", age=35, salary=70000),
    Row(id=5, name="David", age=40, salary=None),
    Row(id=6, name="Eve", age=28, salary=80000)
]

# Create a DataFrame from the sample dataset
df = spark.createDataFrame(data)

# Display the DataFrame
df.show()

# COMMAND ----------

# Import PySpark SQL functions
from pyspark.sql.functions import *

# Register a temporary view
df.createOrReplaceTempView("people")

# Query the view using Spark SQL
spark.sql("SELECT * FROM people").display()

# COMMAND ----------

import pandas as pd

# Sample DataFrame
data = {
    'A': [1, 2, None, 4],
    'B': [None, 2, 3, 4],
    'C': [1, None, None, 4]
}

df = pd.DataFrame(data)

# Function to count the number of null values in each column
def count_nulls(df):
    return df.isnull().sum()

# Function to measure the completeness of the data as a percentage
def data_completeness(df):
    return df.notnull().mean() * 100

# Fill null values with a specified value
def fill_nulls(df, value):
    return df.fillna(value)

# Drop rows that contain any null values
def drop_nulls(df):
    return df.dropna()

# Filter rows where a specific column contains null values
def filter_nulls(df, column):
    return df[df[column].isnull()]

# Example usage
print("Original DataFrame:")
print(df)

print("\nCount of null values in each column:")
print(count_nulls(df))

print("\nData completeness (%):")
print(data_completeness(df))

print("\nDataFrame after filling nulls with 0:")
print(fill_nulls(df, 0))

print("\nDataFrame after dropping rows with nulls:")
print(drop_nulls(df))

print("\nRows where column 'A' has null values:")
print(filter_nulls(df, 'A'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling Null Values

# COMMAND ----------

# Fill null values in specified columns
df.fillna({"age": 0, "city": "Unknown"}).display()

# Drop rows that contain any null values
df.dropna().display()

# Filter rows where the age is null
df.filter(df.age.isNull()).display()

# COMMAND ----------

# Count the nulls per column
from pyspark.sql.functions import sum as spark_sum, when, isnan, col

def count_nulls(df): # defining/creating a function that takes DataFrame df as input
    return df.select([
        spark_sum(when(col(c).isNull() | isnan(c), 1).otherwise(0)).alias(c) # col(c) references each column and checks for Null or NaN values
        for c in df.columns # loops through all the columns
    ])
count_nulls(df).display()

def data_completeness(df): # defining/creating a function that takes DataFrame df as input
    total_rows = df.count()
    return df.select([
        ((total_rows - spark_sum(when(col(c).isNull() | isnan(c), 1).otherwise(0))) / total_rows * 100).alias(f"{c}_complete_pct") # col(c) references each column and checks for Null or NaN values
        for c in df.columns # loops through all the columns
    ])
data_completeness(df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Type Validation

# COMMAND ----------

# Data Type Validation

import pandas as pd

def validate_data_types(df):
    # Check and cast data types
    df['column1'] = df['column1'].astype(int)
    df['column2'] = df['column2'].astype(float)
    
    # Filter out-of-range values for column1
    df = df[(df['column1'] >= 0) & (df['column1'] <= 100)]
    # Filter out-of-range values for column2
    df = df[(df['column2'] >= 0.0) & (df['column2'] <= 100.0)]
    
    return df

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Check data types
df.printSchema()

# Cast to correct types with validation
df.withColumn("age_validated",
    when(col("age").cast("int").isNotNull(), col("age").cast("int"))
    .otherwise(None)
).display()

# Validate numeric ranges
df.filter((col("age") < 0) | (col("age") > 150)).display() # Invalid ages

# COMMAND ----------

# MAGIC %md
# MAGIC ##String Cleaning & Validation

# COMMAND ----------

import re

def clean_string(input_string):
    # Trim whitespace from the beginning and end of the string
    cleaned_string = input_string.strip()
    
    # Remove extra spaces between words in the string
    cleaned_string = re.sub(r'\s+', ' ', cleaned_string)
    
    return cleaned_string

def is_valid_pattern(input_string, pattern):
    # Clean the input string first
    cleaned_string = clean_string(input_string)
    
    # Check if the cleaned string matches the specified pattern
    return bool(re.match(pattern, cleaned_string))

# Example usage
if __name__ == "__main__":
    test_string = "   Hello   World!   "
    pattern = r'^[A-Za-z\s!]+$'  # Validates that the string contains only letters, spaces, and exclamation marks
    
    cleaned = clean_string(test_string)
    print(f"Cleaned String: '{cleaned}'")
    print(f"Is valid pattern: {is_valid_pattern(test_string, pattern)}")

# COMMAND ----------

# Trim whitespace from the name column
df.withColumn("name_clean", trim(col("name"))).display()

# Remove extra spaces from the name column
df.withColumn("name_clean", regexp_replace(col("name"), "\\s+", " ")).display()

# Check for valid patterns (letters only) in the name column
df.filter(~col("name").rlike("^[A-Za-z\\s]+$")).show() # entire string must contain only letters (upper or lowercase) and spaces, with at least one character

# Iterate and update the DataFrame
# Start with the original DataFrame
df_clean = df

# Trim whitespace from the name column
df_clean = df_clean.withColumn("name_clean", trim(col("name")))

# Remove extra spaces from the cleaned column
df_clean = df_clean.withColumn("name_clean", regexp_replace(col("name_clean"), "\\s+", " "))

# Check for valid patterns (letters only) using the cleaned column
print("Names with invalid characters after cleaning:")
df_clean.filter(~col("name_clean").rlike("^[A-Za-z\\s]+$"))

# Optional: Replace the original name column with the cleaned version
df_clean = df_clean.drop("name").withColumnRenamed("name_clean", "name")
df_clean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Duplicate Detection

# COMMAND ----------

import pandas as pd

# Sample DataFrame
data = {
    'A': [1, 2, 2, 3, 4, 4],
    'B': ['a', 'b', 'b', 'c', 'd', 'd'],
    'C': [10, 20, 20, 30, 40, 40]
}

df = pd.DataFrame(data)

# Display the original DataFrame
print("Original DataFrame:")
print(df)

# Remove duplicate rows considering all columns
df_no_duplicates_all = df.drop_duplicates()

# Display the DataFrame after removing duplicates for all columns
print("\nDataFrame after removing duplicates (all columns):")
print(df_no_duplicates_all)

# Remove duplicate rows for specific columns (e.g., columns 'A' and 'B')
df_no_duplicates_specific = df.drop_duplicates(subset=['A', 'B'])

# Display the DataFrame after removing duplicates for specific columns
print("\nDataFrame after removing duplicates (specific columns A and B):")
print(df_no_duplicates_specific)

# COMMAND ----------

# Find duplicate rows in the DataFrame
df.groupBy("name", "age", "city").count().filter(col("count") > 1).display()

# Remove all duplicate rows from the DataFrame
df_deduped = df.dropDuplicates().display()

# Remove duplicates based on the 'name' column
df_deduped_name = df.dropDuplicates(["name"]).display()

# COMMAND ----------

import pandas as pd

# Sample DataFrame
data = {
    'id': [1, 2, 2, 3, 4, 4, 5],
    'name': ['Alice', 'Bob', 'Bob', 'Charlie', 'David', 'David', 'Eve']
}

df = pd.DataFrame(data)

# Display the original DataFrame
print("Original DataFrame:")
print(df)

# Detect duplicate rows
duplicates = df[df.duplicated()]

# Display the duplicate rows
print("\nDuplicates:")
print(duplicates)

# Remove duplicate rows
df_no_duplicates = df.drop_duplicates()

# Display the DataFrame after removing duplicates
print("\nDataFrame after removing duplicates:")
print(df_no_duplicates)

# COMMAND ----------



# COMMAND ----------

import pandas as pd

# Sample DataFrame containing names, ages, and cities
data = {
    'name': ['Alice', 'Bob', 'Alice', 'David', 'Bob', 'Alice'],
    'age': [25, 30, 25, 35, 30, 25],
    'city': ['New York', 'Los Angeles', 'New York', 'Chicago', 'Los Angeles', 'New York']
}

df = pd.DataFrame(data)

# Find duplicate rows based on name, age, and city
duplicates = df.groupby(['name', 'age', 'city']).size().reset_index(name='count')
duplicate_rows = duplicates[duplicates['count'] > 1]

print(duplicate_rows)

# COMMAND ----------

import pandas as pd

# Sample DataFrame
data = {
    'A': [1, 2, 2, 3, 4, 4, 5],
    'B': ['a', 'b', 'b', 'c', 'd', 'd', 'e']
}

df = pd.DataFrame(data)

# Remove duplicate rows while keeping the first occurrence
df_unique = df.drop_duplicates(keep='first')

print(df_unique)

# COMMAND ----------

import pandas as pd

# Sample DataFrame
data = {
    'name': ['Alice', 'Bob', 'Alice', 'Charlie', 'Bob'],
    'age': [25, 30, 25, 35, 30],
    'city': ['New York', 'Los Angeles', 'New York', 'Chicago', 'Los Angeles']
}

df = pd.DataFrame(data)

# Remove duplicates based on the 'name' column, keeping the first occurrence
df_unique = df.drop_duplicates(subset='name', keep='first')

print(df_unique)