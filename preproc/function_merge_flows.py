import time
import pandas as pd
import os
from config import CSV_PATH

def consolidation_data(energy_input:str='donnees_energie.csv',
                       weather_observation_input:str='donnees_station_lyon.csv',
                       sunshine_input:str='final_df_30min.csv',
                       file_output:str='dataset_final.csv') :
    """ The final dataframe containing informations related to energy (OpenDRÉ / RTE), the weather opbservations (Meteo France) and sunshine (NASA) """
    perfcounterstart = time.perf_counter()

    PATH_ENERGY = os.path.join(CSV_PATH, energy_input)
    PATH_WEATHER=os.path.join(CSV_PATH,weather_observation_input)
    PATH_SUNSHINE=os.path.join(CSV_PATH,sunshine_input)
    PATH_FILE_OUTPUT=os.path.join(CSV_PATH,file_output)

    df1 = pd.read_csv(PATH_ENERGY) # The dataframe from OpenDRE
    df1.set_index(['numer_sta','date_timestamp'], inplace=True) # Index set on number_sta, timestamp
    df1 = df1[~df1.index.duplicated(keep='first')] # Delete duplicates

    df2 = pd.read_csv(PATH_WEATHER) # The dataframe from Meteo France
    df2.set_index(['numer_sta','date_timestamp'], inplace=True) # Index set on number_sta, timestamp
    df2 = df2[~df2.index.duplicated(keep='first')] # Delete duplicates

    df3 = pd.read_csv(PATH_SUNSHINE) # The dataframe from NASA
    df3.drop(columns=["Latitude","Longitude"], inplace=True) # Drop unecessary columns (duplicates)
    df3.set_index(['numer_sta','date_timestamp'], inplace=True) # Index set on number_sta, timestamp

    
    df_intermediate = pd.concat([df1, df2], axis=1, join="inner") # Merge of dataframes
    df_final = pd.concat([df_intermediate, df3], axis=1, join="inner") 

    df_final = df_final.rename(columns={
    "Région": "region",
    "Consommation (MW)": "consommation",
    "Thermique (MW)": "thermique",
    "Nucléaire (MW)": "nucleaire",
    "Eolien (MW)": "eolien",
    "Solaire (MW)": "solaire",
    "Hydraulique (MW)": "hydraulique",
    "Pompage (MW)": "pompage",
    "Bioénergies (MW)": "bioenergies",
    "Ech. physiques (MW)": "echanges",
    "numer_sta": "n_station",
    "ff": "vitesse_vent",
    "t_c": "temperature",
    "Nom": "nom_station",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Ensoleillement (Wh/m^2)": "valeur",
    })

    df_final.to_csv(PATH_FILE_OUTPUT, index=True)
    print(f'Data consolidated from the 3 data flows / sources in the file {file_output} in the folder {CSV_PATH}')
    print(f'Number of lines in the file : {len(df_final)}.')
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

if __name__ == "__main__":
    consolidation_data()