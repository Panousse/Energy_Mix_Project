import requests
import gzip
import shutil
import glob
import time
import pandas as pd
import numpy as np

csv_path="raw_data/"

def get_meteo_france_data_from_web(start_year:int, end_year:int) :
    """ Get data of Meteo France from the web - CSV files for observations coming from the main stations in France """
    perfcounterstart = time.perf_counter()

    downloaded_files_number=0
    url_root = "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/"

    for year in range(start_year,end_year):
        for month in range(1,13):
            # Send a GET request to the URL - example https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.199701.csv.gz
     
            filename="synop."+str(year)+str(100+month)[1:]
            extension1=".csv.gz"
            url=url_root+filename+extension1
            response = requests.get(url)
    
            # Check if the request was successful
            if response.status_code == 200:

            # Save the content to a file
                with open(csv_path+filename+extension1, 'wb') as f:
                    f.write(response.content)
                    print(f'File {filename+extension1} downloaded successfully.')
                    downloaded_files_number+=1

                with gzip.open(csv_path+filename+extension1, 'rb') as f_in: # Opening the archive
                    with open(csv_path+filename+'.csv', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out) # Copy of the extraction into a csv file
                        print(f'File {filename}.csv extracted successfully in the folder {csv_path} .')
            else:
                print(f'❌Failed to download {url}. Status code: {response.status_code}')

    print(f"Number of files downloaded : {downloaded_files_number}")
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

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

def resampling_meteo_france_data(file_input:str='main_meteo_france_data_frame.csv',
                                 file_output:str='resampled_meteo_france_data_frame.csv',
                                 frequency:str='30 min',
                                 method:str='ffill',
                                 nb_round:int=1) :
    """ Resample the observations coming from the main stations of Meteo France with the desired frequency and join the CSV related to Meteo France stations properties"""
    perfcounterstart = time.perf_counter()

    df = pd.read_csv(csv_path+file_input,
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
        frame_stations = pd.read_csv(csv_path+"postesSynop.csv",sep=";").drop(columns='Altitude') # Delete column 'Altitude" from Meteo France stations dataframe
        frame_stations.rename(columns={"ID": "numer_sta"},inplace=True) # Rename ID with numer_sta for future join
        
        df_outer=new_dataframe.merge(frame_stations, on="numer_sta") # Join between main dataframe and dataframe related to Meteo France stations
        print("Overview of the dataframe :")
        print(df_outer.head(10))
        df_outer.to_csv(csv_path+file_output, index=False)
        print(f'Resampled Meteo France data into the file {file_output} with a frequency / interval of {frequency} and with the method {method}')
        len_file=len(df_outer)
        print(f'Number of lines in the file : {len_file:n}.')
        perfcounterstop = time.perf_counter()
        print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")
    else:
        print('⚠️Unknow method')

def get_energy_data_from_web(region_to_extract:str="Auvergne-Rhône-Alpes", file_output:str="eco2mix-regional-cons-AURA.csv") :
    """ Retrieve energy data from the ODRÉ (Open Data Reseaux-Énergie) - Données éCO2mix régionales consolidées et définitives """
    # Reference : http://www.rte-france.com/fr/eco2mix/eco2mix
    
    perfcounterstart = time.perf_counter()

    url_root ="https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/exports/csv"
    region_param="libelle_region:"+region_to_extract
    params = {'lang':'fr',
                'refine':region_param,
                'facet':'facet(name="libelle_region",disjunctive=true)',
                'timezone':'Europe/Berlin',
                'use_labels':'true',
                'delimiter':';',
            }
    print(f"⌛Launching the download request. This may take a long time...")
    response = requests.get(url_root,params=params,stream=True)
    print(f" Url used : {response.url}")
    
    # Check if the request was successful
    if response.status_code == 200:
        # Save the content to a file
        with open(csv_path+file_output, 'wb') as f:
            count=0
            chunk_size=None
            for chunk in response.iter_content(chunk_size=chunk_size):
                count+=1
                f.write(chunk) # Write chunk into file and purge the memory for large files
                if (count%20==0) : # display message every 20 chunks / interations - limit the verbosity
                    print(f"Still downloading the file")
        print(f'File {file_output} created successfully in the folder {csv_path}.')

        excel_df = pd.read_csv(csv_path+file_output,sep=";",dtype="string",)
        print(f'Number of lines in the file : {len(excel_df)} for the region {region_to_extract}')

        perfcounterstop = time.perf_counter()
        print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")
    else:
        print(f'❌Failed to retrieve data from {url_root}. Status code: {response.status_code}')

def convert_energy_data(file_input:str="eco2mix-regional-cons-AURA.csv",
                        file_output:str="donnees_energie.csv") :
    """ Clean and convert the data coming from OpenDRE / RTE """
    perfcounterstart = time.perf_counter()

    df = pd.read_csv(csv_path+file_input,
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

    df2=df[['date_timestamp','Région','Consommation (MW)','Thermique (MW)','Nucléaire (MW)','Eolien (MW)','Solaire (MW)','Hydraulique (MW)','Pompage (MW)','Bioénergies (MW)','Ech. physiques (MW)']].copy() # Only useful columns are kept
    df2.to_csv(csv_path+file_output, index=False)
    print(f'Data coming from OpenDRE / Enedis stored in the file {file_output} in the folder {csv_path}')
    print(f'Number of lines in the file : {len(df2)}.')
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

def consolidation_data(energy_input:str='donnees_energie.csv',
                       weather_observation_input:str='donnees_station_lyon.csv',
                       sunshine_input:str='data_nasa_power.csv',
                       file_output:str='dataset_final.csv') :
    """ The final dataframe containing informations related to energy (OpenDRÉ / RTE), the weather opbservations (Meteo France) and sunshine (NASA) """
    perfcounterstart = time.perf_counter()

    df1 = pd.read_csv(csv_path+energy_input) # The dataframe from OpenDRE
    df1.set_index('date_timestamp', inplace=True) # Index set on timestamp 
    df2 = pd.read_csv(csv_path+weather_observation_input) # The dataframe from Meteo France
    df2.set_index('date_timestamp', inplace=True) # Index set on timestamp 
    df3 = pd.read_csv(csv_path+sunshine_input) # The dataframe from NASA
    df3.set_index('DateHeure', inplace=True) # Index set on timestamp
    df1 = df1[~df1.index.duplicated(keep='first')] # Delete duplicates 
    df_final = pd.concat([df1, df2, df3], axis=1) # Merge of the 3 dataframe

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
    "Valeur": "valeur",
    })

    df_final.to_csv(csv_path+file_output, index=False)
    print(f'Data consolidated from the 3 data flows / sources in the file {file_output} in the folder {csv_path}')
    print(f'Number of lines in the file : {len(df_final)}.')
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")
 

get_meteo_france_data_from_web(2013, 2023)
resampling_meteo_france_data(frequency='30 min',method='interpolate')
get_energy_data_from_web()
convert_energy_data()
consolidation_data()