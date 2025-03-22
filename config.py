import os

# CSV_PATH=os.getenv("CSV_PATH")

# Chemins des datasets
DATA_PATH = "../../raw_data/aura_data/dataset_final.csv"
# PROCESSED_PATH = "data/processed/cleaned_data.csv"

# Hyperparamètres du modèle
SEQUENCE_LENGTH = 168  # 7 jours
BATCH_SIZE = 64
EPOCHS = 100
LEARNING_RATE = 0.0005

# Targets et features
TARGETS = ["thermique", "nucleaire", "eolien", "solaire", "hydraulique", "bioenergies"]
LAGS = [1, 2, 4, 8, 56]