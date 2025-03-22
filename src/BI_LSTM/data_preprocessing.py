import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler
from config import DATA_PATH, TARGETS, LAGS

# Chargement des données
def load_data():
    df = pd.read_csv(DATA_PATH, parse_dates=["datetime"])

    # 🛠 Remplacement des valeurs manquantes par interpolation linéaire
    df["vitesse_vent"] = df["vitesse_vent"].interpolate(method="linear")
    df["temperature"] = df["temperature"].interpolate(method="linear")

    # Extraction des features temporelles
    df["hour"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.weekday
    df["month"] = df["datetime"].dt.month
    df["day_of_year"] = df["datetime"].dt.dayofyear
    df["week_of_year"] = df["datetime"].dt.isocalendar().week

    # Encodage sinusoïdal des variables temporelles
    df["sin_hour"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["cos_hour"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["sin_day_of_week"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["cos_day_of_week"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
    df["sin_month"] = np.sin(2 * np.pi * df["month"] / 12)
    df["cos_month"] = np.cos(2 * np.pi * df["month"] / 12)

    # Suppression des colonnes inutiles
    columns_to_drop = ['region', 'n_station', 'nom_station', 'latitude', 'longitude', "hour", "day_of_week", "month", "day_of_year", "week_of_year"]
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # Mise en index et rééchantillonnage
    df.set_index('datetime', inplace=True)
    df = df.resample("3h").mean()

    return df

# Prétraitement des données
def preprocess_data(df):
    # Ajout des moyennes et écarts-types glissants
    window_size = 56
    for target in TARGETS:
        df[f"{target}_rolling_mean_{window_size}"] = df[target].rolling(window=window_size).mean()
        df[f"{target}_rolling_std_{window_size}"] = df[target].rolling(window=window_size).std()

    df = df.dropna()

    # Création des LAGS
    for lag in LAGS:
        for target in TARGETS:
            df[f"{target}_lag{lag}"] = df[target].shift(lag)
    df = df.dropna()

    # Mise à jour des features
    features = [col for col in df.columns if col not in TARGETS]
    for target in TARGETS:
        features.append(f"{target}_rolling_mean_{window_size}")
        features.append(f"{target}_rolling_std_{window_size}")

    # Suppression des features à faible variance
    low_variance_features = df[features].std()
    variance_threshold = 0.01
    features = low_variance_features[low_variance_features > variance_threshold].index.tolist()

    # Sélection des features les plus corrélées aux TARGETS
    correlation_matrix = df[features + TARGETS].corr()
    correlation_target = correlation_matrix[TARGETS].abs().mean(axis=1)
    features = correlation_target[correlation_target > 0.1].index.tolist()

    return df, features

# Normalisation
def normalize_data(df, features):
    scaler_X = RobustScaler()
    scaler_y = RobustScaler()

    df[features] = scaler_X.fit_transform(df[features])
    df[TARGETS] = scaler_y.fit_transform(df[TARGETS])

    return df, scaler_X, scaler_y

# Création des séquences
def create_sequences(data, TARGETS, features, seq_length, stride=3):
    X, y = [], []
    for i in range(0, len(data) - seq_length, stride):
        X.append(data.iloc[i:i + seq_length][features].values)
        y.append(data.iloc[i + seq_length][TARGETS].values)
    return np.array(X), np.array(y)
