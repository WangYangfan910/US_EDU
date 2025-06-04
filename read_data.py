########################################################
####################### PACKAGE ########################
########################################################
import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
# import dataclasses
from dataclasses import dataclass
import itertools
from datetime import date






########################################################
##################### CONSTANTS ########################
########################################################






########################################################
#################### DATA STRUCTURE ####################
########################################################

# define the tuple of varaibles for each file type

edu_var = ("user_id",
    "university_raw", 
    "university_name", 
    "education_number", 
    "startdate", 
    "enddate", 
    "degree", 
    "field", 
    "degree_raw", 
    "field_raw", 
    "world_rank", 
    "us_rank",
    "university_country",
    "university_location",
    "updated")

edu_default = ( 0, # "user_id",
                "none", # "university_raw", 
                "none", # "university_name", 
                0, # "education_number", 
                date(2077,9,10), # "startdate", 
                date(2077,9,10), # "enddate", 
                "none", # "degree", 
                "none", # "field", 
                "none", # "degree_raw", 
                "none", # "field_raw", 
                0, # "world_rank", 
                0, # "us_rank",
                "none", # "university_country",
                "none", # "university_location",
                False)# "updated" 


user_var= ("user_id",
    "firstname",
    "lastname",
    "fullname",
    "f_prob", 
    "m_prob", 
    "white_prob", 
    "black_prob", 
    "api_prob", 
    "hispanic_prob", 
    "native_prob", 
    "multiple_prob", 
    "prestige", 
    "highest_degree", 
    "sex_predicted", 
    "ethnicity_predicted", 
    "profile_linkedin_url", 
    "user_location", 
    "user_country", 
    "profile_title", 
    "updated_dt", 
    "numconnections", 
    "profile_summary",
    "updated" )


user_default= (0, # "user_id",
               "none", # "firstname",
                "none", # "lastname",
                "none", # "fullname",
                0.0, # "f_prob", 
                0.0, # "m_prob", 
                0.0, # "white_prob", 
                0.0, # "black_prob", 
                0.0, # "api_prob", 
                0.0, # "hispanic_prob", 
                0.0, # "native_prob", 
                0.0, # "multiple_prob", 
                0.0, # "prestige", 
                "none", # "highest_degree", 
                "none", # "sex_predicted", 
                "none", # "ethnicity_predicted", 
                "none", # "profile_linkedin_url", 
                "none", # "user_location", 
                "none", # "user_country", 
                "none", # "profile_title", 
                "none", # "updated_dt", 
                0, # "numconnections", 
                "none", # "profile_summary",
                False)# "updated" )



pos_var = ("user_id",
    "position_id",
    "company_raw",
    "company_linkedin_url",
    "company_cleaned",
    "location_raw",
    "region",
    "country",
    "state",
    "metro_area",
    "startdate",
    "enddate",
    "title_raw",
    "role_k1500",
    "job_category",
    "role_k50",
    "role_k150",
    "role_k300",
    "role_k500",
    "role_k1000",
    "remote_suitability",
    "weight",
    "description",
    "start_salary",
    "end_salary",
    "seniority",
    "salary",
    "position_number",
    "rcid",
    "company_name",
    "ultimate_parent_rcid",
    "ultimate_parent_company_name",
    "onet_code",
    "onet_title",
    "ticker",
    "exchange",
    "cusip",
    "naics_code",
    "naics_description",
    "ultimate_parent_factset_id",
    "ultimate_parent_factset_name",
    "total_compensation",
    "additional_compensation",
    "title_translated",
    "updated")

pos_default = ( 0, # "user_id",
                0, # "position_id",
                "none", # "company_raw",
                "none", # "company_linkedin_url",
                "none", # "company_cleaned",
                "none", # "location_raw",
                "none", # "region",
                "none", # "country",
                "none", # "state",
                "none", # "metro_area",
                date(2077,9,10), # "startdate",
                date(2077,9,10), # "enddate",
                "none", # "title_raw",
                "none", # "role_k1500",
                "none", # "job_category",
                "none", # "role_k50",
                "none", # "role_k150",
                "none", # "role_k300",
                "none", # "role_k500",
                "none", # "role_k1000",
                0, # "remote_suitability",
                0, # "weight",
                "none", # "description",
                0.0, # "start_salary",
                0.0, # "end_salary",
                0, # "seniority",
                0.0, # "salary",
                0, # "position_number",
                0, # "rcid",
                "none", # "company_name",
                0, # "ultimate_parent_rcid",
                "none", # "ultimate_parent_company_name",
                "none", # "onet_code",
                "none", # "onet_title",
                "none", # "ticker",
                "none", # "exchange",
                "none", # "cusip",
                "none", # "naics_code",
                "none", # "naics_description",
                "none", # "ultimate_parent_factset_id",
                "none", # "ultimate_parent_factset_name",
                0.0, # "total_compensation",
                0.0, # "additional_compensation",
                "none", # "title_translated",
                False)# "updated")
            

skill_var = ("user_id",
    "skill_raw",
    "skill_source",
    "skill_mapped",
    "skill_k25",
    "skill_k50",
    "skill_k75",
    "updated")

skill_default = ( 0, # "user_id",
                "none", # "skill_raw",
                "none", # "skill_source",
                "none", # "skill_mapped",
                "none", # "skill_k25",
                "none", # "skill_k50",
                "none", # "skill_k75",
                False)# "updated")



########################################################
###################### FUNCTIONS #######################
########################################################

# read one user profile file to update df
def update_prof(ids, dir_path, write_path, file):
    
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

        # create dataframes for storing data, 
        dt = {field_name: [field_default] * numrows for (field_name, field_default) in itertools.zip_longest(user_var, user_default)}
        dt["user_id"] = ids.column("user_id")
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
                # update df

                df.iloc[id_index, 0:-1] = group_data.iloc[access_row, :]
                df.loc[id_index, "updated"] = True

                # update the current access row
                access_row += 1 


        df.to_parquet(path_or_buf = "{write_path}/{file}_{igroup}.parquet".format(write_path = write_path, file=file[0:-8], igroup = igroup), index = False)

                


# read one user edu file to update df
# we just people with at most 6 education entries
def update_edu(ids, dir_path, write_path, file):

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

        dt = {field_name: [field_default] * numrows for (field_name, field_default) in itertools.zip_longest(edu_var, edu_default)}
        dt["user_id"] = ids.column("user_id")
        df1 = pd.DataFrame(dt)
        df2 = pd.DataFrame(dt)
        df3 = pd.DataFrame(dt)
        df4 = pd.DataFrame(dt)
        df5 = pd.DataFrame(dt)
        df6 = pd.DataFrame(dt)
        dfs = (df1, df2, df3, df4, df5, df6)

        group_data = handle.read_row_group(igroup).to_pandas()
        
        # select only the subset with user id within the target id range
        group_data = group_data[(group_data["user_id"] >= min_id) & (group_data["user_id"] <= max_id)]

        # select only the subset with at most 6 edu entries
        remove_id = pd.unique(group_data[group_data["education_number"] > 6]["user_id"])
        group_data = group_data[~group_data["user_id"].isin(remove_id)]

        # sort by id and enddate
        group_data.sort_values(["user_id", "enddate"], inplace = True)
        
        data_rows = group_data.shape[0]
        
        # use access_row to record the current scan row
        access_row = 0
        
        # retrieve the id list of this data subset
        data_ids = pd.unique(group_data["user_id"])

        # assumption: assume each id is associated with at most 10 rows
        for eachid in data_ids:

            # check how many rows this id is associated
            sub_data = group_data[access_row:min(access_row+10, data_rows)]
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
                    
                    dfs[iedu-access_row].iloc[id_index, 0:-1] = group_data.iloc[iedu, :]
                    dfs[iedu-access_row].loc[id_index, "updated"] = True
                # move access_row forward to next id
                access_row += id_rows
        
        for edu_num in range(0,6):
            dfs[edu_num].to_parquet(path_or_buf = "{write_path}/{file}_{igroup}_edu{edu_num}.parquet".format(write_path = write_path, file = file[0:-8], igroup = igroup, edu_num = edu_num), index = False)
            



# read one user position file to update df
# take only the first 5 positions
def update_pos(ids, dir_path, write_path, file):
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

        dt = {field_name: [field_default] * numrows for (field_name, field_default) in itertools.zip_longest(pos_var, pos_default)}
        dt["user_id"] = ids.column("user_id")
        df1 = pd.DataFrame(dt)
        df2 = pd.DataFrame(dt)
        df3 = pd.DataFrame(dt)
        df4 = pd.DataFrame(dt)
        df5 = pd.DataFrame(dt)
        dfs = (df1, df2, df3, df4, df5)

        group_data = handle.read_row_group(igroup).to_pandas()
        
        # select only the subset with user id within the target id range
        group_data = group_data[(group_data["user_id"] >= min_id) & (group_data["user_id"] <= max_id)]
        
        # sort by id and enddate
        group_data.sort_values(["user_id", "enddate"], inplace = True)

        data_rows = group_data.shape[0]

        # use access_row to record the current scan row
        access_row = 0
        
        # retrieve the id list of this data subset
        data_ids = pd.unique(group_data["user_id"])
        
        for eachid in data_ids:

            # check how many rows this id is associated
            sub_data = group_data.iloc[access_row:,0]
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

                for ipos in range(access_row, min(access_row+id_rows, access_row+5)):
                    dfs[ipos-access_row].iloc[id_index, 0:-1] = group_data.iloc[ipos, :]
                    dfs[ipos-access_row].loc[id_index, "updated"] = True
                # move access_row forward to next id
                access_row += id_rows

        for pos_num in range(0,5):
            dfs[pos_num].to_parquet(path_or_buf = "{write_path}/{file}_{igroup}_pos{pos_num}.parquet".format(write_path = write_path, file = file[0:-8], igroup = igroup, pos_num = pos_num), index = False)




# read one user skill file to update df
def update_skill(ids, dir_path, write_path, file):

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

        # create dataframes for storing data,
        dt = {field_name: [field_default] * numrows for (field_name, field_default) in itertools.zip_longest(skill_var, skill_default)}
        dt["user_id"] = ids.column("user_id")
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
                # update df
                df.iloc[id_index, 0:-1] = group_data.iloc[access_row, :]
                df.loc[id_index, "updated"] = True
                # update the current access row
                access_row += 1

        df.to_parquet(path_or_buf = "{write_path}/{file}_{igroup}.parquet".format(write_path = write_path, file=file[0:-8], igroup = igroup), index = False)



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
def read_data_separate(dir_path, write_path, file_list):
    
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
            update_edu(ids, dir_path, write_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))
        
        elif "user_part" in file:
            # file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)
            # dt = {"user_id": ids.column("user_id"), "user_prof": pd.Series([user()] * numrows), "updated": [False] * numrows }
            # df = pd.DataFrame(dt)
            update_prof(ids, dir_path, write_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))

        elif "user_position" in file:
            
            # file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)
            # dt = {"user_id": ids.column("user_id"), "pos1": pd.Series([pos()] * numrows), "pos2": pd.Series([pos()] * numrows), "pos3": pd.Series([pos()] * numrows), "pos4": pd.Series([pos()] * numrows), "updated": [False] * numrows }
            # df = pd.DataFrame(dt)
            update_pos(ids, dir_path, write_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))

        elif "user_skill" in file:

            # file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)
            # dt = {"user_id": ids.column("user_id"), "skill": pd.Series([skill()] * numrows), "updated": [False] * numrows }
            # df = pd.DataFrame(dt)
            update_skill(ids, dir_path, write_path, file)
            # df.to_parquet(path = "../data_clean/{file}.parquet".format(file=file))

        print("finish reading {file}".format(file=file))


        

