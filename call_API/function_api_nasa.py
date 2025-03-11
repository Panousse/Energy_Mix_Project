import requests
import pandas as pd
import os
from config import CSV_PATH

path_df_hourly="final_df.csv"
PATH_DFHOURLY = os.path.join(CSV_PATH, path_df_hourly)
stations_csv_path="postesSynop.csv"
PATH_LAT_LONG=os.path.join(CSV_PATH,stations_csv_path)

def call_api_nasa_yearly(
    stations_csv_path:str=PATH_LAT_LONG,
    output_csv_path:str=PATH_DFHOURLY,
    start_year: int=2013,
    end_year: int=2023
):
    """
    Récupère les données horaires de rayonnement solaire (ALLSKY_SFC_SW_DWN)
    via l'API NASA POWER, année par année, pour chaque station listée dans
    le fichier CSV `stations_csv_path`. Les colonnes attendues dans le CSV
    sont : ID, Latitude, Longitude (séparateur ";").

    Paramètres
    ----------
    stations_csv_path : str
        Chemin vers le CSV contenant les colonnes ID, Latitude, Longitude.
    output_csv_path : str
        Chemin du fichier CSV final à générer (par défaut final_df.csv).
    start_year : int
        Année de début (par défaut 2013).
    end_year : int
        Année de fin (par défaut 2023).

    Retour
    ------
    final_df : pd.DataFrame
        Le DataFrame final contenant toutes les stations et toutes les années.
        (DataFrame vide si aucune donnée n'a été récupérée.)
    """

    # 1) Lecture du CSV des stations
    df_stations = pd.read_csv(stations_csv_path, sep=";")

    # 2) Création d'un dictionnaire {station_id: (latitude, longitude)}
    dic_lat_long = {}
    for _, row in df_stations.iterrows():
        station_id = row["ID"]
        lat = row["Latitude"]
        lon = row["Longitude"]
        dic_lat_long[station_id] = (lat, lon)

    # URL de l'API NASA POWER pour des données horaires
    url = "https://power.larc.nasa.gov/api/temporal/hourly/point"

    # 3) Dictionnaire pour stocker les DataFrames de chaque station
    results = {}

    # 4) Boucle sur chaque station
    for station_id, (lat, lon) in dic_lat_long.items():
        print(f"Traitement de la station {station_id} (lat: {lat}, lon: {lon})")

        # Liste temporaire pour stocker les DataFrames de chaque année
        df_station_list = []

        # Parcours de chaque année de start_year à end_year inclus, le end_year+1 c'est bien 2023+1 mais dans range il faut +1 pour avoir 2023
        for year in range(start_year, end_year + 1):
            start = f"{year}0101"   # ex : "20130101"
            end   = f"{year}1231"   # ex : "20131231"

            params = {
                "start": start,
                "end": end,
                "latitude": lat,
                "longitude": lon,
                "parameters": "ALLSKY_SFC_SW_DWN",
                "community": "RE",
                "format": "JSON"
            }

            # Requête API
            response = requests.get(url, params=params)
            data = response.json()

            # Vérification de la présence des données
            if "properties" in data and "parameter" in data["properties"]:
                data_param = data["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]

                # Construction d'un DataFrame
                df_interval = pd.DataFrame(
                    list(data_param.items()),
                    columns=["date_timestamp", "Ensoleillement (Wh/m^2)"]
                )

                # Conversion de la date en datetime et mise en index
                df_interval["date_timestamp"] = pd.to_datetime(
                    df_interval["date_timestamp"],
                    format="%Y%m%d%H"
                )
                df_interval.set_index("date_timestamp", inplace=True)

                # Ajout des colonnes d'identification
                df_interval["station_id"] = station_id
                df_interval["Latitude"] = lat
                df_interval["Longitude"] = lon

                # Ajout au tableau de cette station
                df_station_list.append(df_interval)

                print(f"  Année {year} : {len(df_interval)} enregistrements.")
            else:
                print(f"  Année {year} : aucune donnée trouvée.")

        # Concaténation des années pour cette station
        if df_station_list:
            station_df = pd.concat(df_station_list)
            results[station_id] = station_df
            print(f"Station {station_id} : {len(station_df)} enregistrements au total.\n")
        else:
            print(f"Aucune donnée récupérée pour la station {station_id}.\n")

    # 5) Concaténation de toutes les stations
    if results:
        final_df = pd.concat(results.values())
        print(f"Nombre total de lignes dans final_df : {len(final_df)}")
        final_df.rename(columns={'station_id':'numer_sta'},inplace=True)

        # 6) Export en CSV
        final_df.to_csv(output_csv_path, index=True)
        print(f"Exportation terminée : {output_csv_path}")

        return final_df
    else:
        print("Aucune donnée n'a été récupérée pour l'ensemble des stations.")
        return pd.DataFrame()  # DataFrame vide si aucune donnée

if __name__=="__main__":
    call_api_nasa_yearly(PATH_LAT_LONG)
