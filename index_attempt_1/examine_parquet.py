import pandas as pd
import sys

file_path = sys.argv[1] if len(sys.argv) > 1 else 'your_file.parquet'

try:
    df = pd.read_parquet(file_path)
    print(f"File: {file_path}")
    print(f"Shape: {df.shape} (rows, columns)")
    print(f"\nColumns: {df.columns.tolist()}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nFirst 10 rows:")
    print(df.head(10).to_string())
    
    if df.shape[0] > 10:
        print(f"\n... ({df.shape[0] - 10} more rows)")
        
except Exception as e:
    print(f"Error reading parquet file: {e}")
