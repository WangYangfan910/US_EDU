########################################################
####################### PACKAGE ########################
########################################################
import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import dataclasses
from dataclasses import dataclass
import itertools
from datetime import date






########################################################
##################### CONSTANTS ########################
########################################################






########################################################
#################### DATA STRUCTURE ####################
########################################################

# define data structure to store education and position




@dataclass
class edu:
    university_raw: str = "none"
    university_name: str = "none"
    education_number: np.int32 = np.int32(0)
    startdate: pa.date32 = date(2077, 9, 10)
    enddate: pa.date32 = date(2077, 9, 10)
    degree: str = "none"
    field: str = "none"
    degree_raw: str = "none"
    field_raw: str = "none"
    world_rank: np.double = np.double(0)
    us_rank: np.double = np.double(0)
    university_country: str = "none"
    university_location: str = "none"

    # use this function to copy data from the file
    def update_value(self, data):
        # print(data)
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data[field_name])

@dataclass
class user:
    firstname: str = "none"
    lastname: str = "none"
    fullname: str = "none"
    f_prob: np.double = np.double(0)
    m_prob: np.double = np.double(0)
    white_prob: np.double = np.double(0)
    black_prob: np.double = np.double(0)
    api_prob: np.double = np.double(0)
    hispanic_prob: np.double = np.double(0)
    native_prob: np.double = np.double(0)
    multiple_prob: np.double = np.double(0)
    prestige: np.double = np.double(0)
    highest_degree: str = "none"
    sex_predicted: str = "none"
    ethnicity_predicted: str = "none"
    profile_linkedin_url: str = "none"
    user_location: str = "none"
    user_country: str = "none"
    profile_title: str = "none"
    updated_dt: pa.date32 = date(2077, 9, 10)
    numconnections: np.double = np.double(0)
    profile_summary: str = "none"

    # use this function to copy data from one row of the file
    def update_value(self, data):
        # print(data)
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data[field_name])

@dataclass
class pos:
    position_id: np.int64 = np.int64(0)
    company_raw: str = "none"
    company_linkedin_url: str = "none"
    company_cleaned: str = "none"
    location_raw: str = "none"
    region: str = "none"
    country: str = "none"
    state: str = "none"
    metro_area: str = "none"
    startdate: str = "none"
    enddate: str = "none"
    title_raw: str = "none"
    role_k1500: str = "none"
    job_category: str = "none"
    role_k50: str = "none"
    role_k150: str = "none"
    role_k300: str = "none"
    role_k500: str = "none"
    role_k1000: str = "none"
    remote_suitability: float = 0.0
    weight: float = 0.0
    description: str = "none"
    start_salary: np.double = np.double(0)
    end_salary: np.double = np.double(0)
    seniority: np.int16 = np.int16(0)
    salary: float = 0.0
    position_number: np.int16 = np.int16(0)
    rcid: np.double = np.double(0)
    company_name: str = "none"
    ultimate_parent_rcid: np.double = np.double(0)
    ultimate_parent_company_name: str = "none"
    onet_code: str = "none"
    onet_title: str = "none"
    ticker: str = "none"
    exchange: str = "none"
    cusip: str = "none"
    naics_code: str = "none"
    naics_description: str = "none"
    ultimate_parent_factset_id: str = "none"
    ultimate_parent_factset_name: str = "none"
    total_compensation: float = 0.0
    additional_compensation: float = 0.0
    title_translated: str = "none"

    # use this function to copy data from the file
    def update_value(self, data):
        # print(data)
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data[field_name])

@dataclass
class skill:
    skill_raw: str = "none"
    skill_source: str = "none"
    skill_mapped: str = "none"
    skill_k25: str = "none"
    skill_k50: str = "none"
    skill_k75: str = "none"

    # use this function to copy data from the file
    def update_value(self, data):
        # print(data)
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data[field_name])


########################################################
###################### FUNCTIONS #######################
########################################################

# read one user profile file to update df
def update_prof(ids, dir_path, file):
    
    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    min_id = ids.column("user_id")[0].as_py()
    max_id = ids.column("user_id")[-1].as_py()
    numrows = ids.num_rows

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):
        print("reading row group: ", igroup)

        dt = {"user_id": ids.column("user_id"), "user_prof": pd.Series([user()] * numrows), "updated": [False] * numrows }
        df = pd.DataFrame(dt)

        group_data = handle.read_row_group(igroup).to_pandas()
        
        # select only the subset with user id within the target id range
        group_data = group_data[(group_data["user_id"] >= min_id) & (group_data["user_id"] <= max_id)]
        # data_rows = group_data.shape[0]
        access_row = 0

        # retrieve the id list of this row group, which is unique for user profile
        data_ids = pd.unique(group_data["user_id"])

        
        for eachid in data_ids:
            # check whether this id is in the target id list
            id_index = ids.column("user_id").index(eachid).as_py()
            if id_index == -1:
                # return index -1, this id not in ids, update the access row and continue to next id
                access_row += 1
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # each user has only one row, so use the access row to construct a user object
                # user_prof = user()
                # user_prof.update_value(group_data.loc[access_row,:])
                # update df
                df.loc[id_index, "user_prof"].update_value(group_data.loc[access_row,:])
                df.loc[id_index, "updated"] = True
                # update the current access row
                access_row += 1 
    
        df.to_csv(path_or_buf= = "../data_clean/{file}_{igroup}.csv".format(file=file[0:-8], igroup = igroup), index = False)

                


# read one user edu file to update df
def update_edu(ids, dir_path, file):

    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    min_id = ids.column("user_id")[0].as_py()
    max_id = ids.column("user_id")[-1].as_py()
    numrows = ids.num_rows


    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):

        print("reading row group: ", igroup)

        dt = {"user_id": ids.column("user_id"), "edu1": pd.Series([edu()] * numrows), "edu2": pd.Series([edu()] * numrows), "edu3": pd.Series([edu()] * numrows), "edu4": pd.Series([edu()] * numrows), "updated": [False] * numrows}
        df = pd.DataFrame(dt)

        group_data = handle.read_row_group(igroup).to_pandas()
        
        # select only the subset with user id within the target id range
        group_data = group_data[(group_data["user_id"] >= min_id) & (group_data["user_id"] <= max_id)]
        data_rows = group_data.shape[0]

        # use access_row to record the current scan row
        access_row = 0
        
        # retrieve the id list of this data subset
        data_ids = pd.unique(group_data["user_id"])

        # assumption: assume each id is associated with at most 20 rows
        for eachid in data_ids:

            # check how many rows this id is associated
            sub_data = group_data[access_row:min(access_row+20, data_rows)]
            # use id_rows to record the num of rows associated with one id
            id_rows = sub_data[sub_data["user_id"] == eachid].shape[0]

            # check whether this id is in the target id list
            id_index = ids.column("user_id").index(eachid).as_py()
            if id_index == -1:
                # return index -1, this id not in ids, update continue to next id
                access_row += id_rows
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # we know the id corresponds to rows: access_row:access_row+id_rows-1

                # a user may have multiple education histories, so iterate through it
                # we take the most recent 4 education experiences

                for iedu in range(access_row, min(access_row+id_rows, access_row+4)):
                    # user_edu = edu()
                    # user_edu.update_value(group_data.loc[iedu,:])
                    df.loc[id_index, "edu"+str(4-(iedu-access_row))].update_value(group_data.loc[iedu,:])
                    df.loc[id_index, "updated"] = True
                # move access_row forward to next id
                access_row += id_rows
        df.to_csv(path_or_buf = "../data_clean/{file}_{igroup}.parquet".format(file=file[0:-8], igroup = igroup), index = False)
            



# read one user position file to update df
def update_pos(ids, dir_path, file):
    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    min_id = ids.column("user_id")[0].as_py()
    max_id = ids.column("user_id")[-1].as_py()
    numrows = ids.num_rows

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):
        print("reading row group: ", igroup)

        dt = {"user_id": ids.column("user_id"), "pos1": pd.Series([pos()] * numrows), "pos2": pd.Series([pos()] * numrows), "pos3": pd.Series([pos()] * numrows), "pos4": pd.Series([pos()] * numrows), "updated": [False] * numrows }
        df = pd.DataFrame(dt)

        group_data = handle.read_row_group(igroup).to_pandas()
        
        # select only the subset with user id within the target id range
        group_data = group_data[(group_data["user_id"] >= min_id) & (group_data["user_id"] <= max_id)]
        data_rows = group_data.shape[0]

        # use access_row to record the current scan row
        access_row = 0
        
        # retrieve the id list of this data subset
        data_ids = pd.unique(group_data["user_id"])
        
        for eachid in data_ids:

            # check how many rows this id is associated
            sub_data = group_data[access_row:min(access_row+20, data_rows)]
            # use id_rows to record the num of rows associated with one id
            id_rows = sub_data[sub_data["user_id"] == eachid].shape[0]

            # check whether this id is in the target id list
            id_index = ids.column("user_id").index(eachid).as_py()
            if id_index == -1:
                # return index -1, this id not in ids, update continue to next id
                access_row += id_rows
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # we know the id corresponds to rows: access_row:access_row+id_rows-1

                # a user may have multiple education histories, so iterate through it
                # we take the most recent 4 education experiences

                for ipos in range(access_row, min(access_row+id_rows, access_row+4)):
                    # user_edu = edu()
                    # user_edu.update_value(group_data.loc[iedu,:])
                    df.loc[id_index, "pos"+str(4-(ipos-access_row))].update_value(group_data.loc[ipos,:])
                    df.loc[id_index, "updated"] = True
                # move access_row forward to next id
                access_row += id_rows

        df.to_csv(path_or_buf = "../data_clean/{file}_{igroup}.parquet".format(file=file[0:-8], igroup = igroup), index = False)




# read one user skill file to update df
def update_skill(ids, dir_path, file):

    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    min_id = ids.column("user_id")[0].as_py()
    max_id = ids.column("user_id")[-1].as_py()
    numrows = ids.num_rows

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):
        print("reading row group: ", igroup)

        dt = {"user_id": ids.column("user_id"), "skill": pd.Series([skill()] * numrows), "updated": [False] * numrows }
        df = pd.DataFrame(dt)

        group_data = handle.read_row_group(igroup).to_pandas()
        
        # select only the subset with user id within the target id range
        group_data = group_data[(group_data["user_id"] >= min_id) & (group_data["user_id"] <= max_id)]
        # data_rows = group_data.shape[0]
        access_row = 0

        # retrieve the id list of this row group, which is unique for user profile
        data_ids = pd.unique(group_data["user_id"])

        
        for eachid in data_ids:
            # check whether this id is in the target id list
            id_index = ids.column("user_id").index(eachid).as_py()
            if id_index == -1:
                # return index -1, this id not in ids, update the access row and continue to next id
                access_row += 1
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # each user has only one row, so use the access row to construct a user object
                # user_prof = user()
                # user_prof.update_value(group_data.loc[access_row,:])
                # update df
                df.loc[id_index, "skill"].update_value(group_data.loc[access_row,:])
                df.loc[id_index, "updated"] = True
                # update the current access row
                access_row += 1

        df.to_csv(path_or_buf = "../data_clean/{file}_{igroup}.parquet".format(file=file[0:-8], igroup = igroup), index = False)



# read one data file and update df
# ids is the target user id list, already sorted in ascending order
def read_one_file(file_path, ids, df):

    # if "education" in file_path:
    #     update_edu(ids, file_path, df)
    # elif "user_part" in file_path:
    #     update_prof(ids, file_path, df)
    # elif "user_position" in file_path:
    #     update_pos(ids, file_path, df)
    # elif "user_skill" in file_path:
    #     update_skill(ids, file_path, df)
    return



   


# It's assumed that a folder "US_EDUC" exists in dir_path and contains all the user data
# construct the full sample
def construct_user_data(dir_path):

    # read the target id data
    ids = pq.read_table("{dir_path}/unique_user_id_US_EDUC.parquet".format(dir_path=dir_path))
    # sort user id: later would useful in filtering
    ids = ids.sort_by("user_id")

    print("user id list read")

    # create an empty DataFrame according to user_id
    numrows = ids.num_rows
    dt = {"user_id": ids.column("user_id"), "user_prof": pd.Series([user()] * numrows), "skill": pd.Series([skill()] * numrows), "edu1": pd.Series([edu()] * numrows), "edu2": pd.Series([edu()] * numrows), "edu3": pd.Series([edu()] * numrows), "edu4": pd.Series([edu()] * numrows), "pos1": pd.Series([pos()] * numrows), "pos2": pd.Series([pos()] * numrows), "pos3": pd.Series([pos()] * numrows), "pos4": pd.Series([pos()] * numrows) }
    df = pd.DataFrame(dt)
    # set the datatype of each column
    # df.astype({"user_id":np.int64, "user_prof":user, "skill":skill, "edu1":edu, "edu2":edu, "edu3":edu, "edu4":edu, "pos1": pos, "pos2": pos, "pos3":pos, "pos4":pos })

    print("df created")

    file_list = os.listdir("{dir_path}/US_EDUC".format(dir_path=dir_path))

    # iterate file_list, read the data and update df
    for file in file_list:
        print("start reading {file}".format(file=file))
        read_one_file("{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file), ids, df)
        print("finish reading {file}".format(file=file))
    return df

# reads each file separately and write to data
def read_data_separate(dir_path, file_list):
    
    # read the target id data
    ids = pq.read_table("{dir_path}/unique_user_id_US_EDUC.parquet".format(dir_path=dir_path))
    # sort user id: later would useful in filtering
    ids = ids.sort_by("user_id")
    # numrows = ids.num_rows

    print("user id list read")

    # # obtain file list
    # file_list = os.listdir("{dir_path}/US_EDUC".format(dir_path=dir_path))

    for file in file_list:

        print("start reading {file}".format(file=file))

        if "education" in file:
            # file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)
            # dt = {"user_id": ids.column("user_id"), "edu1": pd.Series([edu()] * numrows), "edu2": pd.Series([edu()] * numrows), "edu3": pd.Series([edu()] * numrows), "edu4": pd.Series([edu()] * numrows), "updated": [False] * numrows}
            # df = pd.DataFrame(dt)
            update_edu(ids, dir_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))
        
        elif "user_part" in file:
            # file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)
            # dt = {"user_id": ids.column("user_id"), "user_prof": pd.Series([user()] * numrows), "updated": [False] * numrows }
            # df = pd.DataFrame(dt)
            update_prof(ids, dir_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))

        elif "user_position" in file:
            
            # file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)
            # dt = {"user_id": ids.column("user_id"), "pos1": pd.Series([pos()] * numrows), "pos2": pd.Series([pos()] * numrows), "pos3": pd.Series([pos()] * numrows), "pos4": pd.Series([pos()] * numrows), "updated": [False] * numrows }
            # df = pd.DataFrame(dt)
            update_pos(ids, dir_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))

        elif "user_skill" in file:

            # file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)
            # dt = {"user_id": ids.column("user_id"), "skill": pd.Series([skill()] * numrows), "updated": [False] * numrows }
            # df = pd.DataFrame(dt)
            update_skill(ids, dir_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))

        print("finish reading {file}".format(file=file))


        

