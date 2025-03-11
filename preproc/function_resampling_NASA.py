import os
from config import CSV_PATH
import pandas as pd

name_finaldf="final_df.csv"
name_30mins="final_df_30min.csv"
CSV_PATH_INPUT = os.path.join(CSV_PATH, name_finaldf)
CSV_PATH_OUTPUT=os.path.join(CSV_PATH,name_30mins)

# 1) Lecture du CSV initial

def conversion_30mins(PATH_INPUT=CSV_PATH_INPUT,PATH_OUTPUT=CSV_PATH_OUTPUT):
    df = pd.read_csv(PATH_INPUT)

    # 2) Conversion de la colonne date en datetime
    df["date_timestamp"] = pd.to_datetime(df["date_timestamp"])

    # Liste où l’on va stocker les DataFrames resamplés
    df_list = []

    # 3) Groupby par numer_sta
    for numer_sta, group in df.groupby("numer_sta"):
        # Tri par ordre chronologique
        group = group.sort_values("date_timestamp")

        # Mise en index sur date_timestamp
        group = group.set_index("date_timestamp")

        # Optionnel : supprimer ou agréger les doublons d'index si nécessaire
        group = group[~group.index.duplicated(keep='first')]
        # ou group = group.groupby(level=0).mean() pour agréger

        # 4) Resample en 30 minutes + forward fill
        group_30 = group.resample("30min").ffill()

        #interpoler : group_30 = group.resample("30T").interpolate(method='linear'))

        # Réassigner numer_sta, Latitude, Longitude
        group_30["numer_stat"] = numer_sta

        # Si Latitude et Longitude sont constants pour la station :
        group_30["Latitude"] = group["Latitude"].iloc[0]
        group_30["Longitude"] = group["Longitude"].iloc[0]

        df_list.append(group_30)

    # 5) Concaténer toutes les stations en un seul DataFrame
    final_df_30min = pd.concat(df_list)

    # Conserver date_timestamp comme index (si vous le souhaitez sous forme de colonne, utilisez reset_index())
    # final_df_30min.reset_index(inplace=True)

    # 6) Exporter le résultat
    final_df_30min.to_csv(PATH_OUTPUT, index=True)
    print("Exportation terminée : final_df_30min.csv")

    return final_df_30min

if __name__=="__main__":
    conversion_30mins()
