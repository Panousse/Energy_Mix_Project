from data_preprocessing import load_data, preprocess_data, normalize_data, create_sequences
from model_training import build_lstm_model, train_model
from model_evaluation import evaluate_model, plot_predictions
from config import SEQUENCE_LENGTH, BATCH_SIZE, EPOCHS, TARGETS
import numpy as np

# Chargement et prétraitement des données
df = load_data()
df = preprocess_data(df)
features = [col for col in df.columns if col not in TARGETS]
df, scaler_X, scaler_y = normalize_data(df, features)

# Séparation des ensembles
train_size = int(0.7 * len(df))
val_size = int(0.15 * len(df))
df_train, df_val, df_test = df.iloc[:train_size], df.iloc[train_size:train_size + val_size], df.iloc[train_size + val_size:]

# Création des séquences
X_train, y_train = create_sequences(df_train, TARGETS, SEQUENCE_LENGTH)
X_val, y_val = create_sequences(df_val, TARGETS, SEQUENCE_LENGTH)
X_test, y_test = create_sequences(df_test, TARGETS, SEQUENCE_LENGTH)

# Construction du modèle
input_shape = (SEQUENCE_LENGTH, len(features))
model = build_lstm_model(input_shape)

# Entraînement du modèle
model, history = train_model(model, X_train, y_train, X_val, y_val, BATCH_SIZE, EPOCHS)

# Évaluation du modèle
metrics, y_pred_real, y_test_real = evaluate_model(model, X_test, y_test, scaler_y)

# Visualisation des résultats
plot_predictions(y_test_real, y_pred_real)
