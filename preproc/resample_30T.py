

import os
from config import CSV_PATH
import pandas as pd

path_df_hourly="final_df.csv"
path_df_30mins="final_df_30min.csv"
PATH_DFHOURLY = os.path.join(CSV_PATH, path_df_hourly)
PATH_DF30MINS=os.path.join(CSV_PATH,path_df_30mins)

# 1) Lecture du CSV initial

def conversion_30mins(PATH_DFHOURLY):
    df = pd.read_csv(PATH_DFHOURLY)

    # 2) Conversion de la colonne date en datetime
    df["date_timestamp"] = pd.to_datetime(df["date_timestamp"])

    # Liste où l’on va stocker les DataFrames resamplés
    df_list = []

    # 3) Groupby par station_id
    for station_id, group in df.groupby("station_id"):
        # Tri par ordre chronologique
        group = group.sort_values("date_timestamp")

        # Mise en index sur date_timestamp
        group = group.set_index("date_timestamp")

        # Optionnel : supprimer ou agréger les doublons d'index si nécessaire
        group = group[~group.index.duplicated(keep='first')]
        # ou group = group.groupby(level=0).mean() si vous voulez agréger

        # 4) Resample en 30 minutes + forward fill
        group_30 = group.resample("30min").ffill()

        #interpoler : group_30 = group.resample("30T").interpolate(method='linear'))

        # Réassigner station_id, Latitude, Longitude (surtout utiles si un forward fill)
        group_30["station_id"] = station_id

        # Si Latitude et Longitude sont constants pour la station :
        group_30["Latitude"] = group["Latitude"].iloc[0]
        group_30["Longitude"] = group["Longitude"].iloc[0]

        df_list.append(group_30)

    # 5) Concaténer toutes les stations en un seul DataFrame
    final_df_30min = pd.concat(df_list)

    # Conserver date_timestamp comme index (si vous le souhaitez sous forme de colonne, utilisez reset_index())
    # final_df_30min.reset_index(inplace=True)

    # 6) Exporter le résultat
    final_df_30min.to_csv(PATH_DF30MINS, index=True)
    print("Exportation terminée : final_df_30min.csv")

    return final_df_30min

if __name__=="__main__":
    conversion_30mins(PATH_DFHOURLY)
