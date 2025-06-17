# import packages
import read_data as rd
import pandas as pd
import pyarrow.parquet as pq
import os
import multiprocessing


# set constants and data path
DATA_PATH = "../data"
WRITE_PATH = "../data_clean"

# obtain file list
file_list = os.listdir("{dir_path}/US_EDUC".format(dir_path=DATA_PATH))

# partition file list into education, user_profile, user_skill, and user_position subsets
edu_list = [file for file in file_list if "education" in file]
prof_list = [file for file in file_list if "user_part" in file]
pos_list = [file for file in file_list if "user_position" in file]
skill_list = [file for file in file_list if "user_skill" in file]


# read the target id data
ids = pq.read_table("{dir_path}/unique_user_id_US_EDUC.parquet".format(dir_path=DATA_PATH))
# sort user id: later would useful in filtering
ids = ids.sort_by("user_id")

# try reading on edu file
rd.read_one_file(ids, DATA_PATH, WRITE_PATH, edu_list[0])


# if __name__ == "__main__":
    # creating processes
    # p1 = multiprocessing.Process(target=rd.read_data_separate, args=(DATA_PATH, WRITE_PATH, prof_list))
    # p2 = multiprocessing.Process(target=rd.read_data_separate, args=(DATA_PATH, WRITE_PATH, skill_list))
    # p3 = multiprocessing.Process(target=rd.read_data_separate, args=(DATA_PATH, WRITE_PATH, edu_list))
    # p4 = multiprocessing.Process(target=rd.read_data_separate, args=(DATA_PATH, WRITE_PATH, pos_list))
    # starting processes
    # p1.start()
    # p2.start()
    # p3.start()
    # p4.start()

    # wait until processes are finished
    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()



# read data separately and write to files
# rd.read_data_separate(DATA_PATH, prof_list)
# rd.read_data_separate(DATA_PATH, skill_list)
# rd.read_data_separate(DATA_PATH, edu_list)
# rd.read_data_separate(DATA_PATH, pos_list)



# read data
# df = rd.construct_user_data(DATA_PATH)
# df.to_parquet(path = "../data_clean")