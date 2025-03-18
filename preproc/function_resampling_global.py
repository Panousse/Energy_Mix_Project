import pandas as pd
import os
import time
from config import CSV_PATH

def resampling_data(file_input:str="dataset_final.csv",
                        file_output:str="dataset_final_resampled.csv",
                        frequency:str="3 h") :
    """ Change the resampling """
    
    perfcounterstart = time.perf_counter()

    PATH_FILE_INPUT = os.path.join(CSV_PATH, file_input)
    PATH_FILE_OUTPUT = os.path.join(CSV_PATH, file_output)

    df = pd.read_csv(PATH_FILE_INPUT,
                     index_col=None,
                     sep=",",
                     dtype="string",)

    number_of_regions=df['region'].unique()
    new_dataframe=pd.DataFrame()

    for region in number_of_regions :
        df_sorted=df[df['region']==region].sort_values(by=['date_timestamp']) # Sort by timestamp
        df_sorted['date_timestamp']= pd.to_datetime(df_sorted['date_timestamp']) # Convert to datetime64[ns]
        df_sorted=df_sorted.drop_duplicates(subset=['date_timestamp']) # Delete duplicates of timestamp
        df_sorted.set_index('date_timestamp', inplace=True) # Index set on timestamp for future resampling

        df_sorted['consommation'] = pd.to_numeric(df_sorted['consommation'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['thermique'] = pd.to_numeric(df_sorted['thermique'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['nucleaire'] = pd.to_numeric(df_sorted['nucleaire'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['eolien'] = pd.to_numeric(df_sorted['eolien'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['solaire'] = pd.to_numeric(df_sorted['solaire'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['hydraulique'] = pd.to_numeric(df_sorted['hydraulique'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['pompage'] = pd.to_numeric(df_sorted['pompage'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['bioenergies'] = pd.to_numeric(df_sorted['bioenergies'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['echanges'] = pd.to_numeric(df_sorted['echanges'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['vitesse_vent'] = pd.to_numeric(df_sorted['vitesse_vent'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['temperature'] = pd.to_numeric(df_sorted['temperature'],errors='coerce') # Convert to numeric - NaN in case of error
        df_sorted['valeur'] = pd.to_numeric(df_sorted['valeur'],errors='coerce') # Convert to numeric - NaN in case of error

        df_sorted.drop(columns="region", inplace=True)
        print(df_sorted.info())
        df_resampled = df_sorted.resample(frequency).mean()
        df_resampled['region']=region
        new_dataframe=new_dataframe._append(df_resampled)
    new_dataframe.to_csv(PATH_FILE_OUTPUT, index=True)

    print(f'Data coming from the file {file_output} in the folder {CSV_PATH}')
    print(f'Number of lines in the file : {len(new_dataframe)}.')
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

if __name__ == "__main__":
    resampling_data()
