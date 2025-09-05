#!/bin/bash -l
#SBATCH --job-name=hello
#SBATCH --output=hello_py.out
#SBATCH --error=hello_py.err
#SBATCH --time=00:02:00
#SBATCH --ntasks-per-core=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=normal
#SBATCH --account=a-a145

python3 hello.py
