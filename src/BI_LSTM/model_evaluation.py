import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
from config import TARGETS

def evaluate_model(model, X_test, y_test, scaler_y):
    y_pred = model.predict(X_test)
    y_pred_real = scaler_y.inverse_transform(y_pred)
    y_test_real = scaler_y.inverse_transform(y_test)
    
    metrics = {}
    for i, target in enumerate(TARGETS):
        mae = mean_absolute_error(y_test_real[:, i], y_pred_real[:, i])
        mse = mean_squared_error(y_test_real[:, i], y_pred_real[:, i])
        rmse = np.sqrt(mse)
        mape = mean_absolute_percentage_error(y_test_real[:, i], y_pred_real[:, i])
        
        metrics[target] = {"MAE": mae, "MSE": mse, "RMSE": rmse, "MAPE": mape}
        print(f"{target}: MAE={mae:.2f}, MSE={mse:.2f}, RMSE={rmse:.2f}, MAPE={mape:.2f}")
    
    return metrics, y_pred_real, y_test_real

def plot_predictions(y_test_real, y_pred_real, n_days=50):
    plt.figure(figsize=(12, 6))
    for i, target in enumerate(TARGETS):
        plt.subplot(2, 3, i+1)
        plt.plot(y_test_real[:n_days, i], label="Valeurs réelles", linestyle='dashed')
        plt.plot(y_pred_real[:n_days, i], label="Valeurs prédites")
        plt.title(target)
        plt.legend()
    
    plt.tight_layout()
    plt.show()
