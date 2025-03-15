import requests
import pandas as pd
import os
import time
from config import CSV_PATH

def get_meteo_france_data_aggregated(start_year: int, end_year: int, output_filename: str = "aggregated_meteo_data.csv"):
    """Télécharge et agrège les données de Météo France en un seul fichier CSV sans utiliser io ni gzip."""
    perfcounterstart = time.perf_counter()
    url_root = "https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/"
    frames = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Construction du nom de fichier avec mois sur deux chiffres
            year_month = str(year) + str(100 + month)[1:]
            filename = "synop." + year_month
            extension = ".csv.gz"
            url = url_root + filename + extension
            try:
                response = requests.get(url, timeout=25)
                response.raise_for_status()
                if response.status_code == 200:
                    # Enregistrer le fichier compressé temporairement sur disque
                    temp_file = os.path.join(CSV_PATH, filename + extension)
                    with open(temp_file, 'wb') as f:
                        f.write(response.content)

                    # Lire le fichier CSV compressé directement avec pandas
                    df = pd.read_csv(temp_file, sep=";", compression='gzip')
                    df['periode'] = year_month  # Optionnel : ajouter une colonne pour la période
                    frames.append(df)
                    print(f"Fichier {filename + extension} téléchargé et ajouté.")

                    # Supprimer le fichier temporaire
                    os.remove(temp_file)
                else:
                    print(f"❌Erreur pour {url}: code {response.status_code}")
            except Exception as e:
                print(f"❌Erreur lors du téléchargement de {url} : {e}")

    if frames:
        aggregated_df = pd.concat(frames, ignore_index=True)
        output_path = os.path.join(CSV_PATH, output_filename)
        aggregated_df.to_csv(output_path, index=False, sep=";")
        print(f"Aggregation terminée : {len(aggregated_df)} enregistrements dans le fichier {output_filename}.")
    else:
        print("Aucune donnée téléchargée.")

    perfcounterstop = time.perf_counter()
    print(f"⏰Temps écoulé : {perfcounterstop - perfcounterstart:.4f} s")

if __name__=="__main__":
    get_meteo_france_data_aggregated(2013, 2023)
