########################################################
####################### PACKAGE ########################
########################################################
import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import itertools
import math
from datetime import date
from datetime import datetime






########################################################
##################### CONSTANTS ########################
########################################################






########################################################
#################### DATA STRUCTURE ####################
########################################################

# define the tuple of varaibles for each file type

edu_var = (
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

edu_dtype = (

)

edu_default = ( 
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


user_var= (
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


user_default= (
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



pos_var = (
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

pos_default = ( 
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
            

skill_var = (
    "skill_raw",
    "skill_source",
    "skill_mapped",
    "skill_k25",
    "skill_k50",
    "skill_k75",
    "updated")

skill_default = ( 
                "none", # "skill_raw",
                "none", # "skill_source",
                "none", # "skill_mapped",
                "none", # "skill_k25",
                "none", # "skill_k50",
                "none", # "skill_k75",
                False)# "updated")



########################################################
################## SMALL FUNCTIONS #####################
########################################################

def count_occurrence(df, col, val):
    
    count = 0
    for df_val in df[col]:
        if df_val == val:
            count += 1
    
    return count



def find_index(df, col, val):
    # if found, return the index
    for irow in df.index:
        if df.at[irow, col] == val:
            return irow

    # not found, return -1
    return -1


def make_default_list(default_val, rep_time, num_rows):
    return_lt = []
    if rep_time > 1:
        
        for _ in range(0, num_rows):
            return_lt.append([default_val] * rep_time)
    else:
        for _ in range(0, num_rows):
            return_lt.append(default_val)

    return return_lt




########################################################
###################### FUNCTIONS #######################
########################################################

# read one user profile file to update df
def read_prof(ids, dir_path, write_path, file, row_group, c_per_group, c_rank):
    
    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    # min_id = ids.column("user_id")[0].as_py()
    # max_id = ids.column("user_id")[-1].as_py()
    # numrows = ids.num_rows

    print("reading row group: ", row_group, "part", c_rank)
    
    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    group_data = handle.read_row_group(row_group).to_pandas()

    # find the common ids -- to reduce df size
    common_ids = set(ids["user_id"]).intersection(set(group_data["user_id"]))
    common_ids = list(common_ids)
    # numrows = len(common_ids)

    # select only the subset with common user id
    group_data = group_data[group_data["user_id"].isin(common_ids)]

    # reset the data row index
    leng = group_data.shape[0]
    group_data.index = range(0,leng)

    # divide group_data into c_per_group subsets
    # select a subset of the data based on c_rank
    perleng = int(math.ceil(leng / c_per_group))
    group_data = group_data.iloc[c_rank * perleng : min((c_rank+1) * perleng - 1, leng-1)]
    thisleng = group_data.shape[0]
    group_data.index = range(0,thisleng)

    print("group data extracted")
    

    # create dataframes for storing data, 
    dt = {field_name: make_default_list(field_default, 1, numrows) for (field_name, field_default) in itertools.zip_longest(user_var, user_default)}
    # dt = {field_name: [field_default] * numrows for (field_name, field_default) in itertools.zip_longest(user_var, user_default)}

    dt["user_id"] = common_ids
    df = pd.DataFrame(dt)

    # record col names except user_id and updated
    col_names = df.columns.to_list()
    col_names.remove("user_id")
    col_names.remove("updated")

    print("df created")
    
    access_row = 0

    # retrieve the id list of this row group, which is unique for user profile
    data_ids = pd.unique(group_data["user_id"])

    num_read = 0
    numrows = data_ids.shape[0]
    
    for eachid in data_ids:

        num_read += 1
        # check whether this id is in the target id list
        # id_index = ids.column("user_id").index(eachid).as_py()
        id_index = find_index(df, "user_id", eachid)
        # access_row = find_index(group_data, "user_id", eachid)
        if id_index == -1:
            # return index -1, this id not in ids, update the access row and continue to next id
            access_row += 1
            continue
        else: # this id is in ids, retrieve the relevant info and update df
            
            # each user has only one row, so use the access row to construct a user object
            # update df

            for col in col_names:
            
                df.at[id_index, col] = group_data.at[access_row, col]
            
            df.at[id_index, "updated"] = True

            # update the current access row
            access_row += 1 
        
        # report progress
        if num_read % 1000 == 0:
            print(num_read / numrows * 100, "percent read")


    df.to_parquet(path = "{write_path}/{file}_rowgroup{row_group}_{c_rank}.parquet".format(write_path = write_path, file=file[0:-8], row_group = row_group, c_rank = c_rank), index = False)

                


# read one user edu file to update df
# we just people with at most 4 education entries
def read_edu(ids, dir_path, write_path, file, row_group, c_per_group, c_rank):

    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    # min_id = ids.column("user_id")[0].as_py()
    # max_id = ids.column("user_id")[-1].as_py()
    # numrows = ids.num_rows

    print("reading row group: ", row_group, "part", c_rank)

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    group_data = handle.read_row_group(row_group).to_pandas()
    

    # find the common ids -- to reduce df size
    common_ids = set(ids["user_id"]).intersection(set(group_data["user_id"]))
    common_ids = list(common_ids)
    # numrows = len(common_ids)

    # select only the subset with common user id
    group_data = group_data[group_data["user_id"].isin(common_ids)]

    # select only the subset with user id within the target id range
    # group_data = group_data[(group_data["user_id"] >= min_id) & (group_data["user_id"] <= max_id)]
    
    # select only the subset with at most 6 edu entries
    remove_id = pd.unique(group_data[group_data["education_number"] > 6]["user_id"])
    group_data = group_data[~group_data["user_id"].isin(remove_id)]


    # sort by id and enddate
    group_data = group_data.sort_values(["user_id", "enddate"])

    print("group data sorted")

    # reset data row index
    leng = group_data.shape[0]
    group_data.index = range(0, leng)

    # divide group_data into c_per_group subsets
    # select a subset of the data based on c_rank
    perleng = int(math.ceil(leng / c_per_group))
    group_data = group_data.iloc[c_rank * perleng : min((c_rank+1) * perleng - 1, leng-1)]
    thisleng = group_data.shape[0]
    group_data.index = range(0,thisleng)


    print("group data extracted")

    dt = {field_name: make_default_list(field_default, 5, numrows) for (field_name, field_default) in itertools.zip_longest(edu_var, edu_default)}
    # dt = {field_name: [arr.array("u", (field_default, field_default, field_default, field_default, field_default))] for (field_name, field_default) in itertools.zip_longest(edu_var, edu_default)}

    dt["user_id"] = common_ids
    # df1 = pd.DataFrame(dt)
    # df2 = pd.DataFrame(dt)
    # df3 = pd.DataFrame(dt)
    # df4 = pd.DataFrame(dt)
    # dfs = (df1, df2, df3, df4, df5)
    df = pd.DataFrame(dt)
    # reset df row index
   
   
    # record col names except user_id and updated
    col_names = df.columns.to_list()
    col_names.remove("user_id")
    col_names.remove("updated")

    print("dfs created")
    
    
    
    # data_rows = group_data.shape[0]
    
    # use access_row to record the current scan row
    access_row = 0
    
    # retrieve the id list of this data subset
    data_ids = pd.unique(group_data["user_id"])

    num_read = 0
    numrows = data_ids.shape[0]

    print("start iterating ids")
    # assumption: assume each id is associated with at most 10 rows
    # which is innocuous since we select people with at most 6 education histories
    for eachid in data_ids:

        num_read += 1

        print("reading id:", eachid)

        # check how many rows this id is associated
        # sub_data = group_data[access_row:min(access_row+10, data_rows)]
        # use id_rows to record the num of rows associated with one id
        
        # id_rows = sub_data[sub_data["user_id"] == eachid].shape[0]
        id_rows = count_occurrence(group_data, "user_id", eachid)
        # access_row = find_index(group_data, "user_id", eachid)

        # check whether this id is in the target id list
        # id_index = ids.column("user_id").index(eachid).as_py()
        id_index = find_index(df, "user_id", eachid)
        if id_index == -1:
            # return index -1, this id not in ids, update continue to next id
            access_row += id_rows
            continue
        else: # this id is in ids, retrieve the relevant info and update df
            
            # we know the id corresponds to rows: access_row:access_row+id_rows-1

            # a user may have multiple education histories, so iterate through it
            # we take the most recent 5 education experiences
            # print("start recording")
            for iedu in range(access_row, min(access_row+id_rows, access_row+5)):
                
                # print("id_rows =", id_rows)
                # print("id_index =", id_index)
                # print("iedu =",iedu)
                # print("access_row =", access_row)

                for col in col_names:
                    # print("recording", col)
                    # print("updating id_index =", id_index)
                    # print("before update:", df.at[id_index, col])
                    df.at[id_index, col][iedu-access_row] = group_data.at[iedu, col]
                    # print("after update:", df.at[id_index, col])
                
                df.at[id_index, "updated"][iedu-access_row] = True
            
            # move access_row forward to next id
            access_row += id_rows

        # report progress
        if num_read % 1000 == 0:
            print(num_read / numrows * 100, "percent read")


    # for edu_num in range(0,5):
    df.to_parquet(path = "{write_path}/{file}_rowgroup{row_group}_{c_rank}.parquet".format(write_path = write_path, file=file[0:-8], row_group = row_group, c_rank = c_rank), index = False)        



# read one user position file to update df
# take only the first 5 positions
def read_pos(ids, dir_path, write_path, file, row_group, c_per_group, c_rank):
    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    # min_id = ids.column("user_id")[0].as_py()
    # max_id = ids.column("user_id")[-1].as_py()
    # numrows = ids.num_rows

    print("reading row group: ", row_group, "part", c_rank)
    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    group_data = handle.read_row_group(row_group).to_pandas()


    # find the common ids -- to reduce df size
    common_ids = set(ids["user_id"]).intersection(set(group_data["user_id"]))
    common_ids = list(common_ids)
    # numrows = len(common_ids)


    # select only the subset with common user id
    group_data = group_data[group_data["user_id"].isin(common_ids)]

    # sort by id and enddate
    group_data = group_data.sort_values(["user_id", "enddate"])

    # reset data row index
    leng = group_data.shape[0]
    group_data.index = range(0, leng)

    # divide group_data into c_per_group subsets
    # select a subset of the data based on c_rank
    perleng = int(math.ceil(leng / c_per_group))
    group_data = group_data.iloc[c_rank * perleng : min((c_rank+1) * perleng - 1, leng-1)]
    thisleng = group_data.shape[0]
    group_data.index = range(0,thisleng)

    print("group data extracted")

    dt = {field_name: make_default_list(field_default, 4, numrows) for (field_name, field_default) in itertools.zip_longest(pos_var, pos_default)}
    dt["user_id"] = common_ids
    # df1 = pd.DataFrame(dt)
    # df2 = pd.DataFrame(dt)
    # df3 = pd.DataFrame(dt)
    # df4 = pd.DataFrame(dt)
    # dfs = (df1, df2, df3, df4)

    df = pd.DataFrame(dt)

    # record col names except user_id and updated
    col_names = df.columns.to_list()
    col_names.remove("user_id")
    col_names.remove("updated")

    print("df created")
    

    # data_rows = group_data.shape[0]

    # use access_row to record the current scan row
    access_row = 0
    
    # retrieve the id list of this data subset
    data_ids = pd.unique(group_data["user_id"])

    num_read = 0
    numrows = data_ids.shape[0]
    
    for eachid in data_ids:

        num_read += 1

        # check how many rows this id is associated
        # sub_data = group_data.iloc[access_row:,0]
        # use id_rows to record the num of rows associated with one id

        # id_rows = sub_data[sub_data["user_id"] == eachid].shape[0]
        id_rows = count_occurrence(group_data, "user_id", eachid)

        # check whether this id is in the target id list
        # id_index = ids.column("user_id").index(eachid).as_py()
        id_index = find_index(df, "user_id", eachid)
        # access_row = find_index(group_data, "user_id", eachid)
        if id_index == -1:
            # return index -1, this id not in ids, update continue to next id
            access_row += id_rows
            continue
        else: # this id is in ids, retrieve the relevant info and update df
            
            # we know the id corresponds to rows: access_row:access_row+id_rows-1

            # a user may have multiple work histories, so iterate through it
            # we take the most recent 4 work experiences

            for ipos in range(access_row, min(access_row+id_rows, access_row+4)):
                
                for col in col_names:
                    if (col == "startdate") | (col == "enddate"):
                        df.at[id_index, col][ipos-access_row] = datetime.strptime(group_data.at[ipos, col], "%Y-%m-%d")
                    else:
                        df.at[id_index, col][ipos-access_row] = group_data.at[ipos, col]
                
                df.at[id_index, "updated"][ipos-access_row] = True
            # move access_row forward to next id
            access_row += id_rows

        # report progress
        if num_read % 1000 == 0:
            print(num_read / numrows * 100, "percent read")

    # for pos_num in range(0,4):
    df.to_parquet(path = "{write_path}/{file}_rowgroup{row_group}_{c_rank}.parquet".format(write_path = write_path, file=file[0:-8], row_group = row_group, c_rank = c_rank), index = False)




# read one user skill file to update df
def read_skill(ids, dir_path, write_path, file, row_group, c_per_group, c_rank):

    # construct file path
    file_path = "{dir_path}/US_EDUC/{file}".format(dir_path=dir_path, file = file)

    # obtain the min and max target id
    # min_id = ids.column("user_id")[0].as_py()
    # max_id = ids.column("user_id")[-1].as_py()
    # numrows = ids.num_rows

    print("reading row group: ", row_group, "part", c_rank)

    # establish the data input stream
    handle = pq.ParquetFile("{file_path}".format(file_path=file_path))
    group_data = handle.read_row_group(row_group).to_pandas()

    # find the common ids -- to reduce df size
    common_ids = set(ids["user_id"]).intersection(set(group_data["user_id"]))
    common_ids = list(common_ids)
    # numrows = len(common_ids)


    # select only the subset with common user id
    group_data = group_data[group_data["user_id"].isin(common_ids)]


    # reset data row index
    leng = group_data.shape[0]
    group_data.index = range(0, leng)

    # divide group_data into c_per_group subsets
    # select a subset of the data based on c_rank
    perleng = int(math.ceil(leng / c_per_group))
    group_data = group_data.iloc[c_rank * perleng : min((c_rank+1) * perleng - 1, leng-1)]
    thisleng = group_data.shape[0]
    group_data.index = range(0,thisleng)

    print("group data extracted")

    # create dataframes for storing data,
    dt = {field_name: make_default_list(field_default, 1, numrows) for (field_name, field_default) in itertools.zip_longest(skill_var, skill_default)}
    dt["user_id"] = common_ids
    df = pd.DataFrame(dt)

    col_names = df.columns.to_list()
    col_names.remove("user_id")
    col_names.remove("updated")

    print("df created")

    access_row = 0

    # retrieve the id list of this row group, which is unique for user profile
    data_ids = pd.unique(group_data["user_id"])

    num_read = 0
    numrows = data_ids.shape[0]
    
    for eachid in data_ids:

        num_read += 1
        # check whether this id is in the target id list
        # id_index = ids.column("user_id").index(eachid).as_py()
        id_index = find_index(df, "user_id", eachid)
        # access_row = find_index(group_data, "user_id", eachid)
        if id_index == -1:
            # return index -1, this id not in ids, update the access row and continue to next id
            access_row += 1
            continue
        else: # this id is in ids, retrieve the relevant info and update df
            
            # each user has only one row, so use the access row to construct a user object
            # update df
            for col in col_names:
                df.at[id_index, col] = group_data.at[access_row, col]
            
            df.at[id_index, "updated"] = True
            # update the current access row
            access_row += 1

        # report progress
        if num_read % 1000 == 0:
            print(num_read / numrows * 100, "percent read")

    df.to_parquet(path = "{write_path}/{file}_rowgroup{row_group}_{c_rank}.parquet".format(write_path = write_path, file=file[0:-8], row_group = row_group, c_rank = c_rank), index = False)



# read one data file and update df
# ids is the target user id list, already sorted in ascending order
def read_one_group(ids, dir_path, write_path, file, row_group, c_per_group, c_rank):

    if "education" in file:
        read_edu(ids, dir_path, write_path, file, row_group, c_per_group, c_rank)
    elif "user_part" in file:
        read_prof(ids, dir_path, write_path, file, row_group, c_per_group, c_rank)
    elif "user_position" in file:
        read_pos(ids, dir_path, write_path, file, row_group, c_per_group, c_rank)
    elif "user_skill" in file:
        read_skill(ids, dir_path, write_path, file, row_group, c_per_group, c_rank)
    return


