########################################################
####################### PACKAGE ########################
########################################################

from mpi4py import MPI
import numpy as np
import pyarrow.parquet as pq
import os
import read_data as rd



########################################################
##################### CONSTANTS ########################
########################################################

DATA_PATH = "../data"
WRITE_PATH = "../data_clean"


########################################################
###################### Node Info #######################
########################################################

comm = MPI.COMM_WORLD
num_process = comm.size # should be the same as the number of files
rank = comm.Get_rank()



########################################################
#################### Master Node #######################
########################################################

if rank == 0:
    # ids = pq.read_table("{dir_path}/unique_user_id_US_EDUC.parquet".format(dir_path=DATA_PATH))
    # file_list = os.listdir("{dir_path}/US_EDUC".format(dir_path=DATA_PATH))
    print("this is process:", rank, "of", num_process)

    # send ids and file names to worker nodes
    for node_num in range(1,num_process):
        data = {"msg", node_num}
        # data = {'ids': ids, 'file': file_list[node_num]}
        comm.send(data, dest=node_num, tag=11)
    
    # use the remaining resource to read one file
    # rd.read_one_file(ids, DATA_PATH, WRITE_PATH, file_list[0])




########################################################
#################### Worker Node #######################
########################################################

elif rank == 1:
    data = comm.recv(source=0, tag=11)
    # ids = data["ids"]
    # file = data["file"]
    # rd.read_one_file(ids, DATA_PATH, WRITE_PATH, file)
    print("this is process:", data["msg"], "of", num_process)