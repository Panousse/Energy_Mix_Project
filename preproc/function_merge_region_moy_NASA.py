import os
from config import CSV_PATH
import pandas as pd

synoperegions="postesSynop.csv"
name_30mins="final_df_30min.csv"
name_30minsregionmoy="30mins_region_moy.csv"
path_df30mins = os.path.join(CSV_PATH, name_30mins)
path_synop = os.path.join(CSV_PATH,synoperegions)
path_30minsoutput=os.path.join(CSV_PATH,name_30minsregionmoy)

def merge30mins_regions(PATH_INPUT1=path_df30mins,PATH_INPUT2=path_synop,PATH_OUTPUT=path_30minsoutput):

    df1=pd.read_csv(PATH_INPUT1,sep=",") #lecture du df nasa 30mins
    df2=pd.read_csv(PATH_INPUT2,sep=";",encoding="latin-1") #lecture du fichier avec region/ID/Lat//ong

    df2.rename(columns={"ID":"numer_sta"},inplace=True)
    df2.rename(columns={"Région":"region"},inplace=True)

#Check que c'est le bon type
    df1["numer_sta"] = df1["numer_sta"].astype(str)
    df2["numer_sta"] = df2["numer_sta"].astype(str)
# On merge et on supprime les colonnes en doublons
    df_merged = pd.merge(df1, df2, on="numer_sta", how="left")
    df_merged.drop(["Altitude","Longitude_y","Latitude_y","Latitude_x","Longitude_x","numer_stat"],axis=1,inplace=True)
# Enfin on fait la moyenne par date et région
    df_grouped=df_merged.groupby(["region", "date_timestamp"], as_index=False).mean(numeric_only=True).round(2)

    df_grouped.to_csv(PATH_OUTPUT, index=False)
    print(f"Exportation terminée : {name_30minsregionmoy}")

    return df_grouped


if __name__=="__main__":
    merge30mins_regions()
