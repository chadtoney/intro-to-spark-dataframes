# Databricks notebook source
# MAGIC %md
# MAGIC # Joining Dataframes

# COMMAND ----------

import pandas as pd

# Create a DataFrame for individuals and their corresponding cities
people_data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'City': ['New York', 'Los Angeles', 'Chicago']
}
people_df = pd.DataFrame(people_data)

# Create a DataFrame for cities and their associated states
cities_data = {
    'City': ['New York', 'Los Angeles', 'Chicago', 'Houston'],
    'State': ['NY', 'CA', 'IL', 'TX']
}
cities_df = pd.DataFrame(cities_data)

# Display the DataFrames
print("People DataFrame:")
print(people_df)
print("\nCities DataFrame:")
print(cities_df)

# COMMAND ----------

# Example data for Spark DataFrames
data1 = [
    {"name": "Alice", "age": 30, "city": "New York"},
    {"name": "Bob", "age": 25, "city": "Chicago"},
    {"name": "Charlie", "age": 35, "city": "San Francisco"},
    {"name": "David", "age": 40, "city": "Carbondale"}
]
df1 = spark.createDataFrame(data1)

data2 = [
    {"city": "New York", "state": "NY"},
    {"city": "Chicago", "state": "IL"},
    {"city": "San Francisco", "state": "CA"},
    {"city": "Los Angeles", "state": "CA"}
]
df2 = spark.createDataFrame(data2)

display(df1)
display(df2)

# COMMAND ----------

# Example of an inner join in Python using pandas

import pandas as pd

# Create two sample DataFrames
df1 = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'name': ['Alice', 'Bob', 'Charlie', 'David']
})

df2 = pd.DataFrame({
    'id': [3, 4, 5, 6],
    'age': [25, 30, 35, 40]
})

# Perform an inner join on the 'id' column to combine the DataFrames
inner_joined_df = pd.merge(df1, df2, on='id', how='inner')

# Display the resulting DataFrame after the inner join
print(inner_joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner Join - only matching records

# COMMAND ----------

# Perform an inner join on the 'city' column between df1 and df2
inner_join = df1.join(df2, on="city", how="inner")
inner_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join
# MAGIC
# MAGIC A left join returns all rows from the left DataFrame, and the matched rows from the right DataFrame. Unmatched rows from the right will have nulls.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left Join - all records from the left DataFrame

# COMMAND ----------

# Perform a left join on the 'city' column from df1 and df2
left_join = df1.join(df2, on="city", how="left")
left_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join
# MAGIC
# MAGIC A right join returns all rows from the right DataFrame, and the matched rows from the left DataFrame. Unmatched rows from the left will have nulls.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Right Join - All records from right DataFrame

# COMMAND ----------

# Perform a right join on the 'city' column between df1 and df2
right_join = df1.join(df2, on="city", how="right")
right_join.display()

# COMMAND ----------

import pandas as pd

# Sample DataFrames for demonstration
df1 = pd.DataFrame({
    'key': ['A', 'B', 'C'],
    'value1': [1, 2, 3]
})

df2 = pd.DataFrame({
    'key': ['B', 'C', 'D'],
    'value2': [4, 5, 6]
})

# Perform a Full Outer Join on the 'key' column
result = pd.merge(df1, df2, on='key', how='outer')

# Display the result of the merge
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full Outer Join - all records from both DataFrames

# COMMAND ----------

# Perform a full outer join on the 'city' column
full_join = df1.join(df2, on="city", how="outer")
full_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join on Multiple Columns
# MAGIC
# MAGIC You can join DataFrames on multiple columns by passing a list of column names to the `on` parameter. For example:
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join on Multiple Columns

# COMMAND ----------

# Example data for joining on multiple columns: employee information
employee_data = [
    {"emp_id": 1, "dept": "IT", "name": "Alice"},
    {"emp_id": 2, "dept": "HR", "name": "Bob"},
    {"emp_id": 3, "dept": "IT", "name": "Charlie"}
]
employees = spark.createDataFrame(employee_data)

# Example data for employee salaries
salary_data = [
    {"emp_id": 1, "dept": "IT", "salary": 70000},
    {"emp_id": 2, "dept": "HR", "salary": 65000},
    {"emp_id": 3, "dept": "IT", "salary": 75000}
]
salaries = spark.createDataFrame(salary_data)

# COMMAND ----------

# Perform an inner join on multiple columns: 'emp_id' and 'dept'
multi_join = employees.join(salaries, on=["emp_id", "dept"], how="inner")
multi_join.display()

# COMMAND ----------

import pandas as pd

# Sample DataFrames
df1 = pd.DataFrame({
    'A': [1, 2, 3],
    'B': ['a', 'b', 'c']
})

df2 = pd.DataFrame({
    'C': [1, 2, 4],
    'D': ['x', 'y', 'z']
})

# Merging the DataFrames on different column names
result = pd.merge(df1, df2, left_on='A', right_on='C')

# Dropping the duplicate column 'C' from the result
result = result.drop(columns=['C'])

print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join with Different Column Names

# COMMAND ----------

# Example data for joining on different column names
people_data = [
    {"person_name": "Alice", "home_city": "New York"},
    {"person_name": "Bob", "home_city": "Chicago"}
]
people = spark.createDataFrame(people_data)

# Example data for city locations with populations
location_data = [
    {"city_name": "New York", "population": 8000000},
    {"city_name": "Chicago", "population": 2700000}
]
locations = spark.createDataFrame(location_data)

# COMMAND ----------

# Join on different column names using a join expression
diff_cols_join = people.join(locations, people.home_city == locations.city_name, how="inner")
diff_cols_join.display()

# Remove duplicate columns after the join operation
clean_join = people.join(locations, people.home_city == locations.city_name, how="inner") \
                .drop("city_name")
clean_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Self-Join
# MAGIC
# MAGIC A self-join is when a DataFrame is joined with itself. This is useful for hierarchical relationships, such as employees and their managers. 
# MAGIC
# MAGIC Here is an example of how to perform a self-join using pandas in Python:
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join a DataFrame with itself

# COMMAND ----------

# Sample data representing the employee-manager relationship for a self-join
employee_mgr_data = [
    {"emp_id": 1, "name": "Alice", "manager_id": None},
    {"emp_id": 2, "name": "Bob", "manager_id": 1},
    {"emp_id": 3, "name": "Charlie", "manager_id": 1},
    {"emp_id": 4, "name": "Diana", "manager_id": 2}
]
emp_mgr = spark.createDataFrame(employee_mgr_data)

# COMMAND ----------

# Import PySpark SQL functions
from pyspark.sql.functions import *
# Self-join to retrieve manager names

emp_with_mgr = emp_mgr.alias("emp").join(
    emp_mgr.alias("mgr"),
    col("emp.manager_id") == col("mgr.emp_id"),
    how="left"
).select(
    col("emp.emp_id"),
    col("emp.name").alias("employee_name"),
    col("mgr.name").alias("manager_name")
)
emp_with_mgr.display()