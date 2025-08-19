# Databricks notebook source
# MAGIC %md
# MAGIC # Intro to Spark DataFrames
# MAGIC
# MAGIC This notebook demonstrates common operations with PySpark DataFrames, including creation, selection, filtering, column manipulation, string and date functions, renaming, and aggregation. Each section is explained with markdown for clarity.
# MAGIC
# MAGIC ## Table of Contents
# MAGIC 1. [Setup](#setup)
# MAGIC 2. [Creating DataFrames](#creating-dataframes)
# MAGIC 3. [Selecting Columns](#selecting-columns)
# MAGIC 4. [Filtering Rows](#filtering-rows)
# MAGIC 5. [Column Manipulation](#column-manipulation)
# MAGIC 6. [String Functions](#string-functions)
# MAGIC 7. [Date Functions](#date-functions)
# MAGIC 8. [Renaming Columns](#renaming-columns)
# MAGIC 9. [Aggregation](#aggregation)
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Create DataFrame from List") \
    .getOrCreate()

# Sample data: a list of dictionaries
# Each dictionary represents a row with column names as keys
data = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Cathy", "age": 28}
]

# Create a DataFrame from the list of dictionaries
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()

# COMMAND ----------

# Create a simple list of dictionaries
# Each dictionary represents a row with column names as keys

data = [
    {"name": "Alice", "age": 30, "city": "New York"},
    {"name": "Bob", "age": 25, "city": "Chicago"},
    {"name": "Charlie", "age": 35, "city": "San Francisco"}
]

# Convert the Python data structure to a DataFrame
df = spark.createDataFrame(data)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Select Columns
# MAGIC
# MAGIC This section demonstrates how to select one or more columns from the DataFrame, similar to SQL SELECT statements.
# MAGIC

# COMMAND ----------

# Select columns

df.select("name").show()
df.select("name", "city").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Filter Rows
# MAGIC
# MAGIC This section shows how to filter rows in the DataFrame using various conditions, including string operations and SQL expressions.
# MAGIC
# MAGIC ### Example: Filtering Rows Based on Conditions
# MAGIC

# COMMAND ----------

# Filter rows
# Like a WHERE statement in SQL

df.filter(df.age > 30).show()
df.filter(df.city == "Chicago").show()

# Import the upper function from pyspark.sql.functions
from pyspark.sql.functions import upper
df.filter(upper(df.city) == "CHICAGO").show()

# City not equal to Chicago
df.filter(df.city != "Chicago").show()

# Multiple conditions with &
df.filter((df.age >= 30) & (df.city == "New York")).show()

# Multiple conditions with |
df.filter((df.age >= 30) | (df.city == "New York")).show()

# String operations
df.filter(df.name.contains("a")).show()
df.filter(df.name.startswith("A")).show()
df.filter(df.name.endswith("e")).show()

# Filter using isin function for multiple values
df.filter(df.name.isin(["Alice", "Charlie"])).show()

# Filter using isNull and isNotNull
df.filter(df.name.isNotNull()).show()
df.filter(df.name.isNull()).show()

# Filter using between
df.filter(df.age.between(25, 35)).show()

# Filter using SQL expression
df.filter("age = 25 AND city = 'Chicago'").show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, concat, lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Add and Transform Columns") \
    .getOrCreate()

# Sample DataFrame with names and IDs
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Id"])

# Add a new column 'Uppercase_Name' that converts 'Name' to uppercase
df = df.withColumn("Uppercase_Name", upper(col("Name")))

# Add a new column 'Is_Adult' indicating if the 'Id' is 2 or greater
df = df.withColumn("Is_Adult", when(col("Id") >= 2, "Yes").otherwise("No"))

# Add a new column 'Full_Info' that concatenates 'Name' and 'Id' with a separator
df = df.withColumn("Full_Info", concat(col("Name"), lit(" - "), col("Id")))

# Display the transformed DataFrame
df.show()

# COMMAND ----------

# Add new columns
from pyspark.sql.functions import *
from pyspark.sql.types import *

# This creates a new column 'age in 5 years' by adding 5 to the age
df = df.withColumn("age in 5 years", df.age + 5)

# This creates a new column 'city_upper' with the city name in uppercase
df = df.withColumn("city_upper", upper(df.city))

# This creates a new column 'name_city' by concatenating name and city
df = df.withColumn("name_city", concat(df.name, lit(" from "), df.city))

# This creates a new column 'age_category' based on age
df = df.withColumn("age_category", 
                   when(df.age < 30, "Young")
                   .when(df.age >= 30, "Adult")
                   .otherwise("Unknown"))

# This creates a new column 'age_doubled' by multiplying age by 2
df = df.withColumn("age_doubled", df.age * 2)

# This creates a new column 'name_length' with the length of the name
df = df.withColumn("name_length", length(df.name))

# This creates a new column 'first_letter' with the first letter of the name
df = df.withColumn("first_letter", substring(df.name, 1, 1))

# This creates a new column 'age_plus_10' by adding 10 to age
# Also creates 'name_lower' with the name in lowercase
# Also creates 'is_chicago' as a boolean indicating if the city is Chicago
df = df.withColumn("age_plus_10", df.age + 10) \
        .withColumn("name_lower", lower(df.name)) \
        .withColumn("is_chicago", df.city == "Chicago")

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. String Operations and Substring
# MAGIC
# MAGIC This section compares Python string slicing with Spark's `substring` function, and shows how to extract parts of strings in DataFrame columns.
# MAGIC
# MAGIC ### Python String Slicing
# MAGIC
# MAGIC In Python, you can easily extract substrings using slicing. For example:
# MAGIC

# COMMAND ----------

# Regular Python strings
text = "Hello"
print(text[0])    # "H" (first character)
print(text[1:3])  # "el" (characters at positions 1 and 2)

# Spark substring() uses 1-based indexing
df = df.withColumn("first_letter", substring(df.name, 1, 1))    # Position 1 corresponds to the first character
df = df.withColumn("first_three", substring(df.name, 1, 3))     # Positions 1 to 3 correspond to the first three characters
df = df.withColumn("second_char", substring(df.name, 2, 1))     # Position 2 corresponds to the second character
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Date Functions
# MAGIC
# MAGIC This section demonstrates how to work with date columns, including adding dates, calculating with dates, and extracting date parts.
# MAGIC
# MAGIC ### Adding Dates
# MAGIC You can add a specific number of days to a date using the `DATEADD` function.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import (
    current_date,
    date_add,
    date_sub,
    add_months,
    months_between,
    year,
    month,
    dayofmonth,
    to_date,
    lit
)

from datetime import date

df = df.withColumn(
    "hire_date",
    to_date(
        lit("2020-01-15"),
        "yyyy-MM-dd"
    )
)
display(df)

# Use date_add function to add 30 days to hire_date
df = df.withColumn("date_plus_30_days", date_add(df.hire_date, 30))
df.display()

# COMMAND ----------

import pandas as pd

# Sample DataFrame
data = {
    'A': [1, 2, 3],
    'B': [4, 5, 6],
    'C': [7, 8, 9]
}
df = pd.DataFrame(data)

# Rename columns by adding a suffix
df.rename(columns=lambda x: x + '_suffix', inplace=True)

print(df)

# COMMAND ----------

# Rename columns

df = df.withColumnRenamed("name", "full_name") \
        .withColumnRenamed("age", "age_in_years") \
        .withColumnRenamed("city", "home_city")

# Add suffix to all columns
# This creates a new list where every column name gets a suffix
# 'for col in' loops through each of the column names
new_columns = [f"{col}_info" for col in df.columns]
df = df.toDF(*new_columns)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Grouping and Aggregation
# MAGIC
# MAGIC This section demonstrates how to perform group-by and aggregation operations, including counts, averages, and conditional aggregations.
# MAGIC
# MAGIC ### Example Code
# MAGIC

# COMMAND ----------

# Group and Aggregate

# Basic Aggregations: count, sum, avg, min, max
df.groupBy("city").count().display()
df.groupBy("city").avg("age").display()

# Multiple Aggregations on the Same Column

df.groupby("city").agg(
    avg("age").alias("avg_age"),
    min("age").alias("min_age"),
    max("age").alias("max_age"),
    count("*").alias("person_count")
).display()

# Statistical Aggregations

df.groupBy().agg(
    stddev("age").alias("age_stddev"),
    variance("age").alias("age_variance")
).display()

# Global Aggregations (No Grouping)

df.agg(
    count("*").alias("total_people"),
    avg("age").alias("overall_avg_age"),
    min("age").alias("youngest"),
    max("age").alias("oldest")
).display()

# Conditional Aggregation

df.groupBy("city").agg(
    count("*").alias("total_people"),
    sum(when(col("age") >= 30, 1).otherwise(0)).alias("adults_30_plus"),
    avg(when(col("age") < 30, col("age"))).alias("avg_age_under_30")
).display()