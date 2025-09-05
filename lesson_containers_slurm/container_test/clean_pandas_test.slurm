#!/bin/bash -l
#SBATCH --job-name=clean_pandas
#SBATCH --output=clean_pandas.out
#SBATCH --error=clean_pandas.err
#SBATCH --time=00:03:00
#SBATCH --partition=debug
#SBATCH --account=a145

echo "Starting clean compartmentalized job..."
./run_python_container.sh my_pandas_analysis.py
echo "Job completed."
