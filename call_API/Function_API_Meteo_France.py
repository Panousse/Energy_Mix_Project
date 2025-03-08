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
