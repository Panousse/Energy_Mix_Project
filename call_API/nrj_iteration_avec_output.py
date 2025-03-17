import pandas as pd
import os
from config import CSV_PATH
from call_API.nrj_API_sans_output import get_energy_data_from_web


def get_energy_for_all_regions():
    # 1) Lecture du fichier des postes avec région/ID/Lat/Long
    synoperegions = "postesSynop.csv"
    path_synop = os.path.join(CSV_PATH, synoperegions)
    df1 = pd.read_csv(path_synop, sep=";", encoding="latin-1")
    df1.rename(columns={"ID": "numer_sta", "Région": "region"}, inplace=True)

    # 2) Extraction des régions uniques à traiter
    unique_regions = df1['region'].unique()
    print("Régions à traiter :", unique_regions)

    # 3) Itération sur chaque région et fusion des DataFrames
    merged_df = pd.DataFrame()

    for region in unique_regions:
        print(f"\nTraitement de la région : {region}")
        df_region = get_energy_data_from_web(region_to_extract=region)
        if not df_region.empty:
            # On ajoute une colonne 'region' pour identifier la provenance
            df_region["region"] = region
            merged_df = pd.concat([merged_df, df_region], ignore_index=True)
        else:
            print(f"Aucune donnée récupérée pour la région {region}")

    # 4) Export du DataFrame consolidé dans un unique fichier CSV
    output_file = os.path.join(CSV_PATH, "eco2mix_regional_all.csv")
    merged_df.to_csv(output_file, index=False, sep=";")
    print(f"\nFichier consolidé créé avec succès : {output_file}")

if __name__ == "__main__":
    get_energy_for_all_regions()
