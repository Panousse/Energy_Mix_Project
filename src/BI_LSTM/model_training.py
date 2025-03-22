import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.regularizers import l2
from config import LEARNING_RATE, TARGETS

def build_bi_lstm_model(input_shape):
    model = Sequential([
        Bidirectional(LSTM(128, activation="tanh", return_sequences=True, kernel_regularizer=l2(0.01)), input_shape=input_shape),
        Dropout(0.2),
        Bidirectional(LSTM(64, activation="tanh", return_sequences=False, kernel_regularizer=l2(0.01))),
        Dropout(0.2),
        Dense(32, activation="relu", kernel_regularizer=l2(0.01)),
        Dense(len(TARGETS), activation="linear")
    ])

    model.compile(optimizer=Adam(LEARNING_RATE=LEARNING_RATE), loss="mse", metrics=["mae"])
    return model

def train_model(model, X_train, y_train, X_val, y_val, batch_size, epochs):
    early_stopping = tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=10, restore_best_weights=True)
    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[early_stopping],
        verbose=1
    )
    return model, history