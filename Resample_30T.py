


import pandas as pd

# 1) Lecture du CSV initial
df = pd.read_csv("/home/panousse/code/Atl6s/Energy_Mix_Project/raw_data/final_df.csv")

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

    # (Si vous préférez interpoler : group_30 = group.resample("30T").interpolate(method='linear'))

    # Réassigner station_id, Latitude, Longitude (surtout utiles si vous faites un forward fill)
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
final_df_30min.to_csv("/home/panousse/code/Atl6s/Energy_Mix_Project/raw_data/final_df_30min.csv", index=True)
print("Exportation terminée : final_df_30min.csv")
