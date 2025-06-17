#!/bin/bash
#SBATCH -o out.out
#SBATCH -e err.err
#SBATCH --job-name=read_data
#SBATCH --account=dctrl-yw555 
#SBATCH -n 22
#SBATCH --mem-per-cpu=48G

# change the account to the user account
# number of tasks should be the same as the number of row groups in the file
# [81, 82, 81, 82, 12, 15, 60, 8, 80, 15, 15, 83, 15, 11, 81, 15, 15, 15, 80, 15, 15, 11]
# for instance, reading file[0] from os.listdir("{dir_path}/US_EDUC".format(dir_path=DATA_PATH)) 
# should set -n 81


# Execute python script (MPI option)
# source and conda lines should point to the user's own miniconda installation and python environment
source ../../miniconda3/bin/activate
conda activate py_env
module load OpenMPI/4.1.6 
mpirun -n $SLURM_NTASKS python run_mpi.py