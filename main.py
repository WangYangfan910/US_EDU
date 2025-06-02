# import packages
import read_data as rd
import pandas as pd


# set constants and data path
DATA_PATH = "../data"


# read data
df = rd.construct_user_data(DATA_PATH)
df.to_parquet(path = "../data_clean")