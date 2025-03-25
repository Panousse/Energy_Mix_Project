import streamlit as st
import numpy as np
import requests
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.dates as mdates


# Liste des cibles et noms affichés
TARGETS = ["thermique", "nucleaire", "eolien", "solaire", "hydraulique", "bioenergies"]
TARGETS_DISPLAY = {
    "thermique": "Thermique",
    "nucleaire": "Nucléaire",
    "eolien": "Éolien",
    "solaire": "Solaire",
    "hydraulique": "Hydraulique",
    "bioenergies": "Bioénergies"
}

# 1) Charger le JSON et et on fait le json_normalize pour pred et real ça fait la lecture du dict
datajson = pd.read_json("/home/panousse/code/Atl6s/Energy_Mix_Project/raw_data/response_1742743962991.json")

df_pred = pd.json_normalize(datajson["pred"])  # Chaque clé (thermique, nucleaire, ...) en colonne
df_real = pd.json_normalize(datajson["real"])

# 2) La clé step en timestamp, les step dans le json sont pour 3h
#    On part du 1er janvier 2023 à minuit, et chaque step correspond à 3 heures
start_time = datetime(2023, 1, 1, 0, 0, 0)
df_pred["timestamp"] = df_pred["step"].apply(lambda s: start_time + timedelta(hours=s * 3))
df_real["timestamp"] = df_real["step"].apply(lambda s: start_time + timedelta(hours=s * 3))

# 3) Mettre "timestamp" en index
df_pred.set_index("timestamp", inplace=True)
df_real.set_index("timestamp", inplace=True)

# 4) Interface Streamlit
st.title("Prédiction du mix énergétique - Région Rhône-Alpes")

# Sélection des sources d'énergie
options = st.multiselect(
    "Quels moyens de production voulez-vous visualiser ?",
    options=TARGETS,
    format_func=lambda x: TARGETS_DISPLAY[x])

# Sélecteur du nombre de jours à afficher
n_days = st.slider(":spiral_calendar_pad: Combien de jours voulez-vous visualiser ?", 1, 50, 30)# 30 place par défaut le curseur

# Fonction pour tracer la courbe de targets sélectionnées plus haut
def plot_prediction(target_name, df_r, df_p, n_days):
    """
    Trace la courbe des valeurs réelles (df_r) et prédites (df_p) pour la colonne target_name,
    sur n_days jours (1 jour = 8 steps de 3h).
    """

    # Calcul du nombre total de points (3h * 8 = 24h)
    n_steps = n_days * 8

    # valeurs réelles et prédites sur la plage demandée
    real = df_r[target_name].iloc[:n_steps]
    pred = df_p[target_name].iloc[:n_steps]

    # Applique un style
    plt.style.use("ggplot")

    #Figure,axe, avec une taille adaptée
    fig, ax = plt.subplots(figsize=(10, 5))

    # Trace les séries
    ax.plot(real.index, real.values, label="Valeurs réelles", linestyle="dashed")
    ax.plot(pred.index, pred.values, label="Valeurs prédites", linewidth=1.5)

    # Personnalise l'axe des X (dates qui se chevauchent pas)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))                # tous les 1 jour
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))           # Format AAAA-MM-JJ
    plt.setp(ax.get_xticklabels(), rotation=45)                              # Incliner les labels pour éviter le chevauchement

    # Titres, labels et grille
    ax.set_title(TARGETS_DISPLAY[target_name], fontsize=14)
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Consommation / Production", fontsize=12)
    ax.legend(loc="upper left")
    ax.grid(True)

    # Ajustement des marges
    plt.tight_layout()

    # Afficher dans Streamlit
    st.pyplot(fig)

# 5) Bouton pour lancer l'affichage des prédictions ou l'appel de l'API
if st.button("Lancer la prédiction", use_container_width=True):
    if not options:
        st.warning(":warning: Veuillez sélectionner au moins une source d'énergie. :warning:")
    else:
        st.markdown(":white_check_mark: Prédictions générées pour :")
        st.markdown(f":mag_right: Sources sélectionnées : {', '.join([TARGETS_DISPLAY[t] for t in options])}")

        # Boucle sur chaque cible sélectionnée et tracé
        for target in options:
            plot_prediction(target, df_real, df_pred, n_days)

        # Appel API (exemple) à insérer ici si besoi
        # response = requests.get("http://localhost:8080/predict?days={n_days}")
        # data = response.json()
        # ...
