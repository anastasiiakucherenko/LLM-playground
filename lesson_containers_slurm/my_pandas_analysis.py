import pandas as pd
import numpy as np

print("=== Pandas Analysis Test ===")
print(f"Pandas version: {pd.__version__}")
print(f"NumPy version: {np.__version__}")

# Create test data
data = {
    'text': ['Hello world', 'Data science', 'HPC computing', 'Container magic'],
    'id': [1, 2, 3, 4],
    'score': [0.8, 0.9, 0.7, 0.95],
    'category': ['greeting', 'science', 'tech', 'dev']
}

df = pd.DataFrame(data)
print(f"\nDataframe shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"\nFirst 2 rows:")
print(df.head(2))

print(f"\nBasic stats:")
print(df['score'].describe())
print("\n=== Analysis Complete ===")
