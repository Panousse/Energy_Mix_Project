import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
import time
import pandas as pd
import os
from config import CSV_PATH

def get_energy_data_from_web(region_to_extract:str="Auvergne-Rhône-Alpes", file_output:str="eco2mix-regional-cons-AURA.csv") :
    """ Retrieve energy data from the ODRÉ (Open Data Reseaux-Énergie) - Données éCO2mix régionales consolidées et définitives """
    # Reference : http://www.rte-france.com/fr/eco2mix/eco2mix
    # Page de récupération des données : https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-cons-def/information/

    perfcounterstart = time.perf_counter()

    PATH_FILE_OUTPUT = os.path.join(CSV_PATH, file_output)

    url_root ="https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/exports/csv"
    region_param="libelle_region:"+region_to_extract
    params = {'lang':'fr',
                'refine':region_param,
                'facet':'facet(name="libelle_region",disjunctive=true)',
                'where' : "date_heure<date'2023'",
                'timezone':'Europe/Berlin', 
                'use_labels':'true',
                'delimiter':';',
            }
    try:
        print(f"⌛Launching the download request. This may take a long time...")
        response = requests.get(url_root,params=params,stream=True, timeout=25)
        print(f" Url used : {response.url}")
        response.raise_for_status()
        # Check if the request was successful
        if response.status_code == 200:
            # Save the content to a file
            with open(PATH_FILE_OUTPUT, 'wb') as f:
                count=0
                chunk_size=None
                for chunk in response.iter_content(chunk_size=chunk_size):
                    count+=1
                    f.write(chunk) # Write chunk into file and purge the memory for large files
                    if (count%20==0) : # Display message every 20 chunks / interations - limit the verbosity
                        print(f"Still downloading the file")
            print(f'File {file_output} created successfully in the folder {CSV_PATH}.')

            excel_df = pd.read_csv(PATH_FILE_OUTPUT, sep=";", dtype="string",)
            print(f'Number of lines in the file : {len(excel_df)} for the region {region_to_extract}')
            excel_df.to_csv(PATH_FILE_OUTPUT, index=False, sep=";")

            perfcounterstop = time.perf_counter()
            print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")
        else:
            print(f"❌Failed to retrieve data from {url_root}. Status code: {response.status_code}")

    except ConnectionError:
        print("❌Connection error. The network is maybe unreachable.")
    except Timeout:
        print("❌Timeout. Please retry later.")
    except RequestException as e:
        print(f"❌An error occured during the request : {e}")
    except Exception as e:
        print(f"❌Unexpected error : {e}")


if __name__=="__main__":
    get_energy_data_from_web()
