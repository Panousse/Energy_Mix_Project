import requests
import gzip
import shutil
import glob
import time
import pandas as pd
import numpy as np

csv_path="raw_data/"



def agregating_meteo_france_data(file_output:str="main_meteo_france_data_frame.csv") :
    """ Merge the monthly CSV files for observations coming from the main stations in France """
    perfcounterstart = time.perf_counter()

    all_files = glob.glob(csv_path+'synop.*.csv') # List of all csv files beginning with "synop"
    li = []
    print(f"Number of monthly CSV files to merge : {len(all_files)}")

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0,sep=";") # Columns of csv files are separated with semicolumns
        df2 = df[['numer_sta','date','t','ff']].copy() # Only useful columns are kept
        df2['t_c'] = (pd.to_numeric(df2['t'],errors='coerce')-273.15).round(1) # Transform the temperature from K to °C  - rounded to 1 decimal places
        df2['ff'] = (pd.to_numeric(df2['ff'],errors='coerce')).round(1) # Wind speed rounded to 1 decimal places
        df2['date_timestamp'] = pd.to_datetime(df2['date'], format='%Y%m%d%H%M%S').dt.floor('min') # Transform date object to datetime64[ns]
        df2['file'] = filename.split("/")[-1] # Add name of file for eventuel debugging
        li.append(df2)

    concat_frame = pd.concat(li, axis=0, ignore_index=True).drop(columns=['date','t','file']) # Concat all flows coming froml files. Delete not necessary columns from Meteo France dataframe
    concat_frame.to_csv(csv_path+file_output, index=False) # Write a csv file without index and with comma separator
    print(f'Agregated Meteo France data into the file {file_output} into the folder {csv_path}.')
    print(f'Number of lines in the file : {len(concat_frame)}.')
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")
