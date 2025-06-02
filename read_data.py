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
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data.column(field_name)[0])


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

    # use this function to copy data from the file
    def update_value(self, data):
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data.column(field_name)[0])

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
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data.column(field_name)[0])

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
        for field_name in dataclasses.asdict(self).keys():
            setattr(self, field_name, data.column(field_name)[0])





########################################################
###################### FUNCTIONS #######################
########################################################

# read one user profile file to update df
def update_prof(ids, file_path, df):
    
    # obtain the min and max target id
    min_id = ids.column("user_id")[0]
    max_id = ids.column("user_id")[-1]

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):
        group_raw = handle.read_row_group(igroup)
        
        # select only the subset with user id within the target id range
        id_filter = (pc.field("user_id") >= min_id) & (pc.field("user_id") <= max_id)
        group_data = group_raw.filter(mask = id_filter, null_selection_behavior = "drop")
    
        # retrieve the id list of this row group
        data_ids = group_data.column("user_id").unique()

        
        for eachid in data_ids:
            # check whether this id is in the target id list
            if ids.column("user_id").index(eachid) == -1:
                # return index -1, this id not in ids, continue to next id
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # filter the user with this id
                expr = pc.field("user_id") == eachid
                thisuser = group_data.filter(mask = expr, null_selection_behavior = "drop")
                # each user has only one row, so use this row to construct a user object
                user_prof = user()
                user_prof.update_value(thisuser.slice(length=1))
                # update df
                df.loc[df["user_id"] == eachid, "user_prof"] = user_prof

                


# read one user edu file to update df
def update_edu(ids, file_path, df):
    # obtain the min and max target id
    min_id = ids.column("user_id")[0]
    max_id = ids.column("user_id")[-1]

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):
        group_raw = handle.read_row_group(igroup)
        
        # select only the subset with user id within the target id range
        id_filter = (pc.field("user_id") >= min_id) & (pc.field("user_id") <= max_id)
        group_data = group_raw.filter(mask = id_filter, null_selection_behavior = "drop")
    
        # retrieve the id list of this row group
        data_ids = group_data.column("user_id").unique()

        
        for eachid in data_ids:
            # check whether this id is in the target id list
            if ids.column("user_id").index(eachid) == -1:
                # return index -1, this id not in ids, continue to next id
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # filter the user with this id
                expr = pc.field("user_id") == eachid
                thisuser = group_data.filter(mask = expr, null_selection_behavior = "drop").sort_by("enddate")
                
                # a user may have multiple education histories, so iterate through it
                # we take the most recent 4 education experiences
                edu_num = thisuser.num_rows
                for iedu in range(0, min(edu_num, 4)):
                    user_edu = edu()
                    user_edu.update_value(thisuser.take([min(edu_num, 4)-1-iedu]))
                    df.loc[df["user_id"] == eachid, "edu"+str(4-iedu)] = user_edu


# read one user position file to update df
def update_pos(ids, file_path, df):
    # obtain the min and max target id
    min_id = ids.column("user_id")[0]
    max_id = ids.column("user_id")[-1]

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):
        group_raw = handle.read_row_group(igroup)
        
        # select only the subset with user id within the target id range
        id_filter = (pc.field("user_id") >= min_id) & (pc.field("user_id") <= max_id)
        group_data = group_raw.filter(mask = id_filter, null_selection_behavior = "drop")
    
        # retrieve the id list of this row group
        data_ids = group_data.column("user_id").unique()

        
        for eachid in data_ids:
            # check whether this id is in the target id list
            if ids.column("user_id").index(eachid) == -1:
                # return index -1, this id not in ids, continue to next id
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # filter the user with this id
                expr = pc.field("user_id") == eachid
                thisuser = group_data.filter(mask = expr, null_selection_behavior = "drop").sort_by("enddate")
                
                # a user may have multiple job histories, so iterate through it
                # we take the earliest 4 work experiences
                pos_num = thisuser.num_rows
                for ipos in range(0, min(pos_num, 4)):
                    user_pos = pos()
                    user_pos.update_value(thisuser.take([min(pos_num, 4)-1-ipos]))
                    df.loc[df["user_id"] == eachid, "pos"+str(4-ipos)] = user_pos


# read one user skill file to update df
def update_skill(ids, file_path, df):
     # obtain the min and max target id
    min_id = ids.column("user_id")[0]
    max_id = ids.column("user_id")[-1]

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    num_row_group = handle.metadata.num_row_groups


    # iterate through the row groups to read data
    for igroup in range(0, num_row_group):
        group_raw = handle.read_row_group(igroup)
        
        # select only the subset with user id within the target id range
        id_filter = (pc.field("user_id") >= min_id) & (pc.field("user_id") <= max_id)
        group_data = group_raw.filter(mask = id_filter, null_selection_behavior = "drop")
    
        # retrieve the id list of this row group
        data_ids = group_data.column("user_id").unique()

        
        for eachid in data_ids:
            # check whether this id is in the target id list
            if ids.column("user_id").index(eachid) == -1:
                # return index -1, this id not in ids, continue to next id
                continue
            else: # this id is in ids, retrieve the relevant info and update df
                
                # filter the user with this id
                expr = pc.field("user_id") == eachid
                thisuser = group_data.filter(mask = expr, null_selection_behavior = "drop")
                # each user has only one row, so use this row to construct a skill object
                user_skill = skill()
                user_skill.update_value(thisuser.slice(length=1))
                # update df
                df.loc[df["user_id"] == eachid, "skill"] = user_skill



# read one data file and update df
# ids is the target user id list, already sorted in ascending order
def read_one_file(file_path, ids, df):

    if "education" in file_path:
        update_edu(ids, file_path, df)
    elif "user_part" in file_path:
        update_prof(ids, file_path, df)
    elif "user_position" in file_path:
        update_pos(ids, file_path, df)
    elif "user_skill" in file_path:
        update_skill(ids, file_path, df)



   


# It's assumed that a folder "US_EDUC" exists in dir_path and contains all the user data
def construct_user_data(dir_path):

    # read the target id data
    ids = pq.read_table("{dir_path}/unique_user_id_US_EDUC.parquet".format(dir_path=dir_path))
    # sort user id: later would useful in filtering
    ids.sort_by("user_id")

    print("user id list read")

    # create an empty DataFrame according to user_id
    numrows = ids.num_rows
    dt = {"user_id": ids.column("user_id"), "user_prof":pd.Series([pd.NA] * numrows), "skill":pd.Series([pd.NA] * numrows), "edu1":pd.Series([pd.NA] * numrows), "edu2":pd.Series([pd.NA] * numrows), "edu3": pd.Series([pd.NA] * numrows), "edu4": pd.Series([pd.NA] * numrows), "pos1": pd.Series([pd.NA] * numrows), "pos2": pd.Series([pd.NA] * numrows), "pos3":pd.Series([pd.NA] * numrows), "pos4":pd.Series([pd.NA] * numrows) }
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





