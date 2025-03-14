import pandas as pd
import os
import time
from config import CSV_PATH

def convert_energy_data(file_input:str="eco2mix-regional-cons-AURA.csv",
                        file_output:str="donnees_energie.csv") :
    """ Clean and convert the data coming from OpenDRE / RTE """
    
    perfcounterstart = time.perf_counter()

    PATH_FILE_INPUT = os.path.join(CSV_PATH, file_input)
    PATH_FILE_OUTPUT = os.path.join(CSV_PATH, file_output)

    df = pd.read_csv(PATH_FILE_INPUT,
                     index_col=None,
                     header=0,sep=";",
                     dtype="string",)

    df['Date - Heure UTC'] = pd.to_datetime(df['Date - Heure'],utc=True) # Conversion to UTC
    df['date_timestamp'] = df['Date - Heure UTC'].dt.tz_localize(None).dt.strftime('%Y-%m-%d %H:%M:%S') # Change the format of the timestamp
    df['date_timestamp']= pd.to_datetime(df['date_timestamp']) # Convert to datetime64[ns]

    df['Consommation (MW)']= pd.to_numeric(df['Consommation (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Thermique (MW)']= pd.to_numeric(df['Thermique (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Numcléaire (MW)']= pd.to_numeric(df['Nucléaire (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Eolien (MW)']= pd.to_numeric(df['Eolien (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Solaire (MW)']= pd.to_numeric(df['Solaire (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Hydraulique (MW)']= pd.to_numeric(df['Hydraulique (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Pompage (MW)']= pd.to_numeric(df['Pompage (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Bioénergies (MW)']= pd.to_numeric(df['Bioénergies (MW)'],errors='coerce') # Convert to numeric - NaN in case of error
    df['Ech. physiques (MW)']= pd.to_numeric(df['Ech. physiques (MW)'],errors='coerce') # Convert to numeric - NaN in case of error

    df2=df[['date_timestamp', 'Région','Consommation (MW)','Thermique (MW)','Nucléaire (MW)','Eolien (MW)','Solaire (MW)','Hydraulique (MW)','Pompage (MW)','Bioénergies (MW)','Ech. physiques (MW)']].copy() # Only useful columns are kept
    df2.to_csv(PATH_FILE_OUTPUT, index=False)
    print(f'Data coming from OpenDRE / RTE stored in the file {file_output} in the folder {CSV_PATH}')
    print(f'Number of lines in the file : {len(df2)}.')
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

if __name__ == "__main__":
    convert_energy_data()
