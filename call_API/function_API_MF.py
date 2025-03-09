import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
import gzip
import shutil
import time
import os
from config import CSV_PATH

def get_meteo_france_data_from_web(start_year:int, end_year:int) :
    """ Get data of Meteo France from the web - CSV files for observations coming from the main stations in France """
    perfcounterstart = time.perf_counter()

    downloaded_files_number=0
    url_root = "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/"

    for year in range(start_year,end_year):
        for month in range(1,13):
            # Send a GET request to the URL - example https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop.199701.csv.gz

            year_month=str(year)+str(100+month)[1:]
            filename="synop."+year_month
            extension1=".csv.gz"
            url=url_root+filename+extension1
            try:
                response = requests.get(url, timeout=25)
                response.raise_for_status()

                # Check if the request was successful
                if response.status_code == 200:

                # Save the content to a file
                    with open(os.path.join(CSV_PATH, filename+extension1), 'wb') as f:
                        f.write(response.content)
                        print(f'File {filename+extension1} downloaded successfully.')
                        downloaded_files_number+=1

                    with gzip.open(os.path.join(CSV_PATH, filename+extension1), 'rb') as f_in: # Opening the archive
                        with open(os.path.join(CSV_PATH, filename+'.csv'), 'wb') as f_out:
                         shutil.copyfileobj(f_in, f_out) # Copy of the extraction into a csv file
                        print(f'File {filename}.csv extracted successfully in the folder {CSV_PATH} .')
                else:
                    print(f'❌Failed to download {url}. Status code: {response.status_code}')
            except ConnectionError:
                print("❌Connection error. The network is maybe unreachable.")
                print(f'{downloaded_files_number} downloaded files until the period {year_month}.')
                return
            except Timeout:
                print("❌Timeout. Please retry later.")
                print(f'{downloaded_files_number} downloaded files until the period {year_month}.')
                return
            except RequestException as e:
                print(f"❌An error occured during the request : {e}")
                print(f'{downloaded_files_number} downloaded files until the period {year_month}.')
                return
            except Exception as e:
                print(f"❌Unexpected error : {e}")
                print(f'{downloaded_files_number} downloaded files until the period {year_month}.')
                return

    print(f"Number of files downloaded : {downloaded_files_number} for the period {start_year} - {end_year}.")
    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

if __name__=="__main__":
    get_meteo_france_data_from_web(2013,2023)
