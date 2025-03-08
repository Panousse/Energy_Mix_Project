
import time
import pandas as pd
import os
from config import CSV_PATH

def resampling_meteo_france_data(file_input:str='main_meteo_france_data_frame.csv',
                                 file_output:str='resampled_meteo_france_data_frame.csv',
                                 frequency:str='30 min',
                                 method:str='ffill',
                                 nb_round:int=1) :
    """ Resample the observations coming from the main stations of Meteo France with the desired frequency and join the CSV related to Meteo France stations properties"""
    perfcounterstart = time.perf_counter()

    PATH_FILE_INPUT = os.path.join(CSV_PATH, file_input)
    PATH_FILE_OUTPUT = os.path.join(CSV_PATH, file_output)

    df = pd.read_csv(PATH_FILE_INPUT,
                     index_col=None,
                     sep=",") # Read csv file
    df['date_timestamp'] = pd.to_datetime(df['date_timestamp'],format='%Y-%m-%d %H:%M:%S') # Transform the date_timestamp column to datetime64[ns]
    stations_list=df['numer_sta'].unique() # List of the unique stations
    print("Beginning of the resampling")
    new_dataframe=pd.DataFrame()

    if(method=='ffill' or method=='mean' or method=='interpolate'):
        for station in stations_list : # For each station
            df_sorted=df[df['numer_sta']==station].sort_values(by=['date_timestamp']) # Sort by timestamp
            df_sorted=df_sorted.drop_duplicates(subset=['date_timestamp']) # Delete duplicates of timestamp
            df_sorted.set_index('date_timestamp', inplace=True) # Index set on timestamp for future resampling
            if (method=='ffill'):
                df_resampled = df_sorted.resample(frequency).ffill() # Forward fill (ffill): propagates the last valid observation forward - For upscaling
            elif (method=='mean'):
                df_resampled = df_sorted.resample(frequency).mean() # For downscaling
            else:
                df_resampled = df_sorted.resample(frequency).interpolate(method='linear') # Fill with interpolation - for upscaling
            new_dataframe=new_dataframe._append(df_resampled)

        new_dataframe['date_timestamp']=new_dataframe.index # Retrieve the value of the index
        new_dataframe['numer_sta'] = new_dataframe['numer_sta'].astype('int') # Convert the station number into integer
        new_dataframe['t_c'] = (pd.to_numeric(new_dataframe['t_c'],errors='coerce')).round(nb_round) # Transform the temperature from K to °C  - rounded to n decimal places
        new_dataframe['ff'] = (pd.to_numeric(new_dataframe['ff'],errors='coerce')).round(nb_round) # Wind speed rounded to n decimal places
        frame_stations = pd.read_csv(CSV_PATH+"postesSynop.csv",sep=";").drop(columns='Altitude') # Delete column 'Altitude" from Meteo France stations dataframe
        frame_stations.rename(columns={"ID": "numer_sta"},inplace=True) # Rename ID with numer_sta for future join

        df_outer=new_dataframe.merge(frame_stations, on="numer_sta") # Join between main dataframe and dataframe related to Meteo France stations
        print("Overview of the dataframe :")
        print(df_outer.head(10))
        df_outer.to_csv(PATH_FILE_OUTPUT, index=False)
        print(f'Resampled Meteo France data into the file {file_output} with a frequency / interval of {frequency} and with the method {method}')
        len_file=len(df_outer)
        print(f'Number of lines in the file : {len_file:n}.')
        perfcounterstop = time.perf_counter()
        print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")
    else:
        print('⚠️Unknow method')

if __name__ == "__main__":
    resampling_meteo_france_data()
