#!/bin/bash -l
#SBATCH --job-name=hello
#SBATCH --output=hello.out
#SBATCH --time=00:01:00
#SBATCH --ntasks-per-core=1
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH --account=a-a145

echo "Hello, world from Slurm!"
