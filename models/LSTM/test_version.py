import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler, RobustScaler

import warnings
warnings.filterwarnings("ignore")

df = pd.read_csv(CSV_PATH, parse_dates=["date_time"])

df = df.rename(columns={'date_time':'datetime',
                   'valeur':'ray_solaire'})

# Extraction des features temporelles
df["hour"] = df["datetime"].dt.hour
df["day_of_week"] = df["datetime"].dt.weekday
df["month"] = df["datetime"].dt.month
df["day_of_year"] = df["datetime"].dt.dayofyear
df["week_of_year"] = df["datetime"].dt.isocalendar().week

# Encodage sinusoïdal des variables temporelles (pour éviter les discontinuités)
df["sin_hour"] = np.sin(2 * np.pi * df["hour"] / 24)
df["cos_hour"] = np.cos(2 * np.pi * df["hour"] / 24)
df["sin_day_of_week"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
df["cos_day_of_week"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
df["sin_month"] = np.sin(2 * np.pi * df["month"] / 12)
df["cos_month"] = np.cos(2 * np.pi * df["month"] / 12)

columns_to_drop = ['region', 'n_station', 'nom_station', 'latitude', 'longitude', "hour", "day_of_week", "month", "day_of_year", "week_of_year"]
df.drop(columns=columns_to_drop, inplace=True)
df.set_index('datetime', inplace=True)

features = [
    "consommation",     
    "vitesse_vent",      
    "temperature",        
    "ray_solaire",       
    "sin_hour", "cos_hour", 
    "sin_day_of_week", "cos_day_of_week", 
    "sin_month", "cos_month" 
]

targets = [
    "thermique",      
    "nucleaire",      
    "eolien",         
    "solaire",        
    "hydraulique",   
    "pompage",        
    "bioenergies",   
    "echanges"        
]

df["vitesse_vent"] = df["vitesse_vent"].interpolate(method="linear")
df["temperature"] = df["temperature"].interpolate(method="linear")

train = df[df.index < "2022-01-01"]
val = df[(df.index >= "2022-01-01") & (df.index < "2023-01-01")]
test = df[df.index >= "2023-01-01"]

scaler_X = RobustScaler()
scaler_Y = RobustScaler()

train_X = scaler_X.fit_transform(train[features])
train_Y = scaler_Y.fit_transform(train[targets])

val_X = scaler_X.transform(val[features])
val_Y = scaler_Y.transform(val[targets])

test_X = scaler_X.transform(test[features])
test_Y = scaler_Y.transform(test[targets])

print(f"Train X Shape: {train_X.shape}, Train Y Shape: {train_Y.shape}")
print(f"Val X Shape: {val_X.shape}, Val Y Shape: {val_Y.shape}")
print(f"Test X Shape: {test_X.shape}, Test Y Shape: {test_Y.shape}")

def create_sequences(data_X, data_Y, seq_length, target_length):
    X, y = [], []
    for i in range(len(data_X) - seq_length - target_length):
        X.append(data_X[i:i+seq_length])  # Fenêtre d'entrée
        y.append(data_Y[i+seq_length:i+seq_length+target_length])  # Fenêtre de sortie multi-target
    return np.array(X), np.array(y)

seq_length = 48  # 24h d'historique (48 points de 30 min)
target_length = 1  # Prédiction à 30 min

X_train, y_train = create_sequences(train_X, train_Y, seq_length, target_length)
X_val, y_val = create_sequences(val_X, val_Y, seq_length, target_length)
X_test, y_test = create_sequences(test_X, test_Y, seq_length, target_length)

X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], len(features)))
X_val = X_val.reshape((X_val.shape[0], X_val.shape[1], len(features)))
X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], len(features)))

model = Sequential([
    LSTM(128, return_sequences=True, input_shape=(seq_length, len(features))),
    Dropout(0.3),
    LSTM(64, return_sequences=False),
    Dropout(0.3),
    Dense(32, activation="relu"),
    Dense(len(targets))  # Une sortie par target
])

model.compile(optimizer="adam", loss="mse", metrics=["mae"])

model.summary()

history = model.fit(X_train, y_train,
                    epochs=30, batch_size=32,
                    validation_data=(X_val, y_val))

loss, mae = model.evaluate(X_test, y_test)
print(f"Test Loss: {loss}, Test MAE: {mae}")

# 📈 Visualisation de la loss

plt.plot(history.history["loss"], label="Train Loss")
plt.plot(history.history["val_loss"], label="Val Loss")
plt.xlabel("Epochs")
plt.ylabel("Loss")
plt.legend()
plt.show()