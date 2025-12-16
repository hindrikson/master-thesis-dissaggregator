#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=100:00:00
#SBATCH --gres=gpu:0
#SBATCH --mem=8G
#SBATCH -o /mnt/home/rmiranda/all/repos/master-thesis-dissaggregator/slurm/logs/%j-%x.log
#SBATCH --job-name=demandregio
#SBATCH --nodelist=gpu3.omnia.cluster
#SBATCH --partition=h100
 
eval "$(conda shell.bash hook)"
conda activate new_disag
 
cd /mnt/home/rmiranda/all/repos/master-thesis-dissaggregator/


srun python3 generate_regional_ts.py


echo "Time Series Generation Completed"
