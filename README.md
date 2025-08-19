# Introduction to Spark DataFrames

This repository contains Python notebooks demonstrating the fundamentals of Apache Spark DataFrames.

## 📚 Contents

- **Notebooks**: Interactive Jupyter notebooks with examples and exercises
- **Data**: Sample datasets used in the tutorials
- **Utils**: Helper functions and utilities

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Jupyter Notebook or JupyterLab
- Apache Spark (instructions provided in setup notebook)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/intro-to-spark-dataframes.git
   cd intro-to-spark-dataframes
   ```

2. Create a virtual environment:
   ```bash
   python -m venv spark-env
   ```

3. Activate the virtual environment:
   - On Windows: `spark-env\Scripts\activate`
   - On macOS/Linux: `source spark-env/bin/activate`

4. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

5. Start Jupyter:
   ```bash
   jupyter notebook
   ```

## 📖 Notebooks

1. **01-Setup-and-Basics.ipynb** - Environment setup and Spark basics
2. **02-DataFrame-Operations.ipynb** - Core DataFrame operations
3. **03-Data-Processing.ipynb** - Data cleaning and transformation
4. **04-Advanced-Operations.ipynb** - Advanced DataFrame techniques

## 📊 Sample Data

The `data/` directory contains sample datasets used throughout the notebooks:
- Customer data (CSV)
- Sales transactions (JSON)
- Product information (Parquet)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
