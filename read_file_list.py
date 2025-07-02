#### This script reads the file list and corresponding row groups in the data directory ####
import os
import pyarrow.parquet as pq

DATA_PATH = "../data"


FILE_LIST = os.listdir("{dir_path}/US_EDUC".format(dir_path=DATA_PATH))

leng = len(FILE_LIST)

for file in range(0, leng):
    
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=DATA_PATH, file = FILE_LIST[file])
    
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    
    num_row_groups = handle.num_row_groups
    
    print(file, FILE_LIST[file], num_row_groups)

