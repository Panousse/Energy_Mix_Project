import requests
import pandas as pd

# Lecture du fichier des stations météo (ID, Latitude, Longitude)
df = pd.read_csv(r"/home/panousse/code/Atl6s/Energy_Mix_Project/raw_data/Lat_Long_Stat.csv", sep=";")

# Création d'un dictionnaire des latitudes et longitudes par station
dic_lat_long = {}
for index, row in df.iterrows():
    ID = row["ID"]
    Latitude = row["Latitude"]
    Longitude = row["Longitude"]
    dic_lat_long[ID] = (Latitude, Longitude)

# URL de l'API NASA POWER
url = "https://power.larc.nasa.gov/api/temporal/hourly/point"

# Dictionnaire pour stocker les résultats finaux pour chaque station
results = {}

# Parcours de chaque station
for station_id, (lat, lon) in dic_lat_long.items():
    print(f"Traitement de la station {station_id} (lat: {lat}, lon: {lon})")
    # Liste temporaire pour stocker les DataFrames de chaque intervalle
    df_station_list = []

    # Boucle sur les intervalles de 2 ans de 2013 à 2023
    # Ici, on traite 2013-2014, 2015-2016, 2017-2018, 2019-2020, puis 2021-2022.
    # Enfin, on traite l'année 2023 séparément si besoin.
    for start_year in range(2013, 2023, 2):
        start = f"{start_year}0101"          # Exemple : "20130101"
        end = f"{start_year + 1}1231"          # Exemple : "20141231"

        params = {
            "start": start,
            "end": end,
            "latitude": lat,
            "longitude": lon,
            "parameters": "ALLSKY_SFC_SW_DWN",
            "community": "RE",
            "format": "JSON"
        }

        response = requests.get(url, params=params)
        data = response.json()
        print(f"  Intervalle {start} à {end} -> Clés de réponse : {list(data.keys())}")

        if "properties" in data and "parameter" in data["properties"]:
            data_param = data["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]
            df_interval = pd.DataFrame(list(data_param.items()),
                                       columns=["date_timestamp", "Ensoleillement (Wh/m^2)"])
            df_interval["date_timestamp"] = pd.to_datetime(df_interval["date_timestamp"],
                                                           format="%Y%m%d%H")
            df_interval.set_index("date_timestamp", inplace=True)
            # Ajout des informations de la station
            df_interval["station_id"] = station_id
            df_interval["Latitude"] = lat
            df_interval["Longitude"] = lon
            df_station_list.append(df_interval)
            print(f"  Données récupérées pour l'intervalle {start} à {end}")

    # Traitement éventuel de l'année 2023 si elle n'est pas incluse dans un intervalle complet de 2 ans
    # Ici, on récupère 2023 séparément.
    start = "20230101"
    end = "20231231"
    params = {
        "start": start,
        "end": end,
        "latitude": lat,
        "longitude": lon,
        "parameters": "ALLSKY_SFC_SW_DWN",
        "community": "RE",
        "format": "JSON"
    }
    response = requests.get(url, params=params)
    data = response.json()
    print(f"  Intervalle {start} à {end} -> Clés de réponse : {list(data.keys())}")
    if "properties" in data and "parameter" in data["properties"]:
        data_param = data["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]
        df_interval = pd.DataFrame(list(data_param.items()),
                                   columns=["date_timestamp", "Ensoleillement (Wh/m^2)"])
        df_interval["date_timestamp"] = pd.to_datetime(df_interval["date_timestamp"],
                                                       format="%Y%m%d%H")
        df_interval.set_index("date_timestamp", inplace=True)
        # Ajout des informations de la station
        df_interval["station_id"] = station_id
        df_interval["Latitude"] = lat
        df_interval["Longitude"] = lon
        df_station_list.append(df_interval)
        print(f"  Données récupérées pour l'intervalle {start} à {end}")

    # Concaténation des DataFrames pour cette station (si des données ont été récupérées)
    if df_station_list:
        results[station_id] = pd.concat(df_station_list)
        print(f"Station {station_id} : {len(results[station_id])} enregistrements au total.\n")
    else:
        print(f"Aucune donnée récupérée pour la station {station_id}.\n")

# Optionnel : concaténer les données de toutes les stations dans un seul DataFrame
if results:
    final_df = pd.concat(results.values())
    print("Aperçu du DataFrame final :")
    print(final_df.head())

    # Exporter en CSV dans le chemin spécifié
    final_df.to_csv("/home/panousse/code/Atl6s/Energy_Mix_Project/final_df.csv", index=True)
    print("Exportation terminée : final_df.csv")
