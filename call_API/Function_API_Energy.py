import requests
import gzip
import shutil
import glob
import time
import pandas as pd
import numpy as np

csv_path="raw_data/"

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
