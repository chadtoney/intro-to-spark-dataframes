# Introduction to Spark DataFrames

This repository contains **Databricks Python notebooks** demonstrating practical Apache Spark DataFrame operations with hands-on examples and clear explanations.

## 📚 What's Inside

This collection provides comprehensive tutorials on **PySpark DataFrames** through three progressive notebooks:

### 📓 Notebooks

**[01 Intro to Spark DataFrames.py](notebooks/01%20Intro%20to%20Spark%20DataFrames.py)**
- Creating DataFrames from Python data structures
- Selecting and filtering data
- Column manipulation and transformations
- String operations and date functions
- Renaming columns and aggregations
- Comprehensive examples with both pandas and PySpark comparisons

**[02 Joining with Spark DataFrames.py](notebooks/02%20Joining%20with%20Spark%20DataFrames.py)**
- Inner joins for matching records only
- Left joins to preserve all left DataFrame records
- Right joins to preserve all right DataFrame records  
- Full outer joins for complete data preservation
- Practical examples with both pandas and PySpark implementations

**[03 Data Quality and Cleaning with Spark DataFrames.py](notebooks/03%20Data%20Quality%20and%20Cleaning%20with%20Spark%20DataFrames.py)**
- Handling null values and missing data
- Data validation and type checking
- String cleaning and standardization
- Duplicate detection and removal
- Real-world data quality scenarios

## 🚀 Getting Started

### Prerequisites

- **Databricks workspace** (recommended) or local Spark environment
- Basic Python knowledge
- Familiarity with data concepts

### Running the Notebooks

**Option 1: Databricks (Recommended)**
1. Import the `.py` files into your Databricks workspace
2. Attach to a Spark cluster
3. Run cells interactively

**Option 2: Local Environment**
1. Clone this repository:
   ```bash
   git clone https://github.com/chadtoney/intro-to-spark-dataframes.git
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Convert `.py` files to notebooks or run directly in your Python environment

## 🎯 Learning Objectives

After working through these notebooks, you'll understand:

- ✅ **DataFrame Creation**: Building DataFrames from various Python data structures
- ✅ **Data Selection**: Filtering rows and selecting columns efficiently
- ✅ **Transformations**: Adding, modifying, and renaming columns
- ✅ **String Operations**: Text manipulation and substring extraction
- ✅ **Date Handling**: Working with date columns and calculations
- ✅ **Joins**: Combining multiple DataFrames with different join types
- ✅ **Aggregations**: Grouping data and calculating summary statistics
- ✅ **Data Quality**: Cleaning messy data and handling edge cases

## � Key Features

- **Practical Examples**: Real-world scenarios with sample data
- **Comparative Learning**: Both pandas and PySpark examples
- **Progressive Difficulty**: From basics to advanced operations
- **Databricks Format**: Ready-to-use in Databricks environment
- **Comprehensive Coverage**: End-to-end DataFrame operations

## 🤝 Contributing

Contributions are welcome! Feel free to:
- Add more examples
- Improve explanations
- Fix issues
- Suggest new topics

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
