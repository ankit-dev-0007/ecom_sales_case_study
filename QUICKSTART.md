# E-commerce Data Processing

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Pipeline

**Full setup and execution:**
```bash
python run.py --setup --reports
```

**Just run the pipeline:**
```bash
python run.py
```

**Run with reports:**
```bash
python run.py --reports
```

### 3. Run Tests
```bash
pytest
```

Or with coverage:
```bash
pytest --cov=src tests/
```

## Databricks Usage

Upload the `src/` directory and `databricks_notebook.ipynb` to your Databricks workspace and run the notebook.

## Module Usage

### Import and use individual modules:

```python
from pyspark.sql import SparkSession
from src.ingestion import load_products_data
from src.transformations import enrich_product_data

spark = SparkSession.builder.getOrCreate()

# Load data
raw_products = load_products_data(spark)

# Transform data
enriched_products = enrich_product_data(raw_products)
```

## Project Structure

- `src/` - Main source code
  - `config.py` - Configuration settings
  - `utils.py` - Utility functions
  - `ingestion.py` - Data loading
  - `transformations.py` - Data transformations
  - `pipeline.py` - Pipeline orchestration
- `tests/` - Test suite
- `run.py` - Main execution script
- `setup_environment.py` - Environment setup
- `databricks_notebook.ipynb` - Simplified Databricks notebook
