import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
import time
import pandas as pd
import csv
from config import CSV_PATH

def get_energy_data_from_web(region_to_extract: str = "Auvergne-Rhône-Alpes") -> pd.DataFrame:
    """
    Récupère les données énergies de l'ODRÉ (éCO2mix régionales consolidées et définitives)
    pour la région spécifiée, et renvoie directement un DataFrame.

    Paramètres
    ----------
    region_to_extract : str
        Nom de la région à extraire (par défaut "Auvergne-Rhône-Alpes").

    Retour
    ------
    pd.DataFrame
        Le DataFrame contenant les données récupérées. Un DataFrame vide est renvoyé en cas d'erreur.
    """
    start_time = time.perf_counter()

    url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/exports/csv"
    params = {
        'lang': 'fr',
        'refine': "libelle_region:" + region_to_extract,
        'facet': 'facet(name="libelle_region",disjunctive=true)',
        'where': "date_heure<date'2023'",
        'timezone': 'Europe/Berlin',
        'use_labels': 'true',
        'delimiter': ';'
    }

    try:
        print(f"⌛ Lancement de la requête pour la région {region_to_extract}...")
        response = requests.get(url, params=params, timeout=25)
        response.raise_for_status()

        # Transformation du texte CSV en DataFrame
        csv_lines = response.text.splitlines()
        reader = csv.reader(csv_lines, delimiter=';')
        data_list = list(reader)
        header = data_list[0]  # Première ligne = entêtes
        data = data_list[1:]
        df = pd.DataFrame(data, columns=header)

        print(f"Nombre de lignes récupérées: {len(df)}")
        elapsed = time.perf_counter() - start_time
        print(f"⌛ Temps écoulé: {elapsed:.4f} s")
        return df

    except (ConnectionError, Timeout, RequestException) as e:
        print(f"❌ Une erreur est survenue lors de la requête: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    df_energy = get_energy_data_from_web()
    print(df_energy.head())
