########################################################
####################### PREWOrg ########################
########################################################

# This script uses MPI to read one data file. It sends one row group to one worker core,
# and each core saves one parquet file of the corresponding row group


########################################################
####################### PACKAGE ########################
########################################################

from mpi4py import MPI
import numpy as np
import pyarrow.parquet as pq
import os
import read_group as rg

########################################################
##################### CONSTANTS ########################
########################################################

DATA_PATH = "../data"
WRITE_PATH = "../data_clean"

FILE_LIST = os.listdir("{dir_path}/US_EDUC".format(dir_path=DATA_PATH))

# TODO: change the i value below for each file, and change the -n config in mpi_read.sh
# i takes values from 0-21, since there are 22 data files
# the corresponding number of row groups are
# [81, 82, 81, 82, 12, 15, 60, 8, 80, 15, 15, 83, 15, 11, 81, 15, 15, 15, 80, 15, 15, 11]
# the number of tasks in the .sh file should be the same as the number of row groups in the data file to be read
i = 0
c_per_group = 10
row_group_list = [81, 82, 81, 82, 12, 15, 60, 8, 80, 15, 15, 83, 15, 11, 81, 15, 15, 15, 80, 15, 15, 11]
file = FILE_LIST[i]
row_group_num = row_group_list[i]
print("in total", row_group_num, "row groups")


########################################################
###################### Node Info #######################
########################################################

comm = MPI.COMM_WORLD
num_process = comm.size # should be the same as the number of row groups in the current file
rank = comm.Get_rank()

if num_process != row_group_num * c_per_group:
    raise(NameError("number of cores not match number of jobs"))


########################################################
#################### Master Node #######################
########################################################

if rank == 0:
    print("this is process:", rank, "of", num_process)

    ids = pq.read_table("{dir_path}/unique_user_id_US_EDUC.parquet".format(dir_path=DATA_PATH)).to_pandas()
    ids.sort_values("user_id", inplace = True)
    # file_list = os.listdir("{dir_path}/US_EDUC".format(dir_path=DATA_PATH))

    # send ids and file names to worker nodes
    for node_num in range(1,num_process):
        
        data = {'ids': ids, 'row_group': node_num // c_per_group, "c_rank": node_num % c_per_group}
        comm.send(data, dest=node_num, tag=11)
    
    # use the remaining resource to read one part
    rg.read_one_group(ids, DATA_PATH, WRITE_PATH, file, 0, c_per_group, 0)




########################################################
#################### Worker Node #######################
########################################################

else:
    print("this is process:", rank, "of", num_process)

    data = comm.recv(source=0, tag=11)
    ids = data["ids"]
    row_group = data["row_group"]
    c_rank = data["c_rank"]
    rg.read_one_group(ids, DATA_PATH, WRITE_PATH, file, row_group, c_per_group, c_rank)











