#!/bin/bash -l
#SBATCH --job-name=pandas_test
#SBATCH --output=pandas_test.out
#SBATCH --error=pandas_test.err
#SBATCH --time=00:05:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=debug
#SBATCH --account=a145

echo "Testing pandas in container..."
podman run --rm docker://jupyter/scipy-notebook:latest python -c "
import pandas as pd
print('Pandas version:', pd.__version__)
# Create test data
data = {'text': ['Hello', 'World'], 'id': [1, 2]}
df = pd.DataFrame(data)
print('Test dataframe:')
print(df)
print('Success!')
"
