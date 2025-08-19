# Introduction to Spark DataFrames

This repository contains **Databricks Python notebooks** demo## 🔧 Key Features

- **Zero External Dependencies**: All data generated from Python lists and dictionaries
- **Enterprise-Ready**: Works in the most restrictive environments
- **Practical Examples**: Real-world scenarios with sample data
- **Comparative Learning**: Both pandas and PySpark examples where relevant
- **Progressive Difficulty**: From basics to advanced operations
- **Databricks Format**: Ready-to-use in Databricks environment
- **Self-Contained**: Everything needed is included in the notebooks
- **Security-Friendly**: No file uploads, external connections, or special permissions requiredng practical Apache Spark DataFrame operations with hands-on examples and clear explanations.

## 🎯 Why These Notebooks Are Different

Most Spark DataFrame tutorials require external dependencies that aren't always available in enterprise environments:
- 📁 **No external CSV files** - Can't upload files or access public datasets
- 🚫 **No DBFS access** - File system restrictions in locked-down environments  
- 🔒 **No dbutils** - Utility functions disabled for security
- 🗄️ **No database connections** - Unknown or restricted SQL database access
- 🌐 **No internet access** - Can't download public datasets

**These notebooks solve that problem.** Every example uses **pure Python data structures** that convert directly to DataFrames, making them perfect for:
- 🏢 **Enterprise training environments** with strict security policies
- 🎓 **Educational settings** without external data access
- 🧪 **Isolated development environments** 
- 👥 **Customer demonstrations** where you can't see their available data sources

**You can run these notebooks anywhere Spark is available - no external dependencies required.**

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
- **That's it!** No external files, databases, or special permissions needed

### Running the Notebooks

**Option 1: Databricks (Recommended)**
1. Import the `.py` files into your Databricks workspace
2. Attach to a Spark cluster
3. Run cells interactively - **everything just works!**

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

### 🎯 Perfect For Enterprise Training

These notebooks were specifically designed for **customer training scenarios** where:
- ✅ You can't see what data sources are available
- ✅ Security policies prevent file uploads
- ✅ External database access is unknown or restricted
- ✅ Internet access for downloading datasets is blocked
- ✅ DBFS and dbutils are disabled

**Every example generates its own sample data** - no external dependencies required!

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

## 👨‍🏫 For Trainers and Educators

These notebooks were born from the challenge of teaching Spark DataFrames in **real enterprise environments** where traditional tutorials simply don't work. If you're:

- 🏢 **Training customers** in locked-down environments
- 🎓 **Teaching students** without access to external data
- 💼 **Doing demos** where you can't see available data sources
- 🔒 **Working in air-gapped** or highly secure environments

These notebooks eliminate the friction. **No setup, no uploads, no external dependencies** - just pure DataFrame learning with immediate results.

### What Makes This Different From Other Tutorials:

| Common Tutorials | These Notebooks |
|-----------------|-----------------|
| 📁 Require CSV uploads | ✅ Generate data from Python lists |
| 🌐 Download external datasets | ✅ Self-contained sample data |
| 🗄️ Connect to databases | ✅ Create DataFrames from dictionaries |
| 🛠️ Use dbutils/DBFS | ✅ Pure PySpark operations |
| 🔐 Need special permissions | ✅ Work with basic Spark access |

**Copy these notebooks anywhere Spark runs - they just work.**

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
