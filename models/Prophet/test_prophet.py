import pandas as pd
import os
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import time
from config import CSV_PATH

PATH_FILE_INPUT = os.path.join(CSV_PATH, "dataset_final_resampled.csv")

perfcounterstart = time.perf_counter()
df = pd.read_csv(PATH_FILE_INPUT)
df2=df[df["region"]=="Auvergne-Rhône-Alpes"].copy()
df2["production_totale"]=df['thermique']+df['nucleaire']+df['eolien']+df['solaire']+df['hydraulique']+df['pompage']+df['bioenergies']
df2.drop(columns=["region","consommation","thermique","nucleaire","eolien","solaire", "hydraulique","pompage","bioenergies","echanges","vitesse_vent","temperature","valeur"],axis=1,inplace=True)
df2.rename(columns={'date_timestamp':'ds',
                   'production_totale':'y'},inplace=True)
train = df2[df2["ds"]< "2022-01-01"] # period 2013 - 2021 included
train.head()
test = df2[df2["ds"]>= "2022-01-01"] # only period 2022
test.head()
m = Prophet()
m.fit(train)

future = m.make_future_dataframe(periods=365*8,freq="3h", include_history=False)

forecast = m.predict(future)
print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
forecast2=forecast[['ds', 'yhat']].set_index('ds')
test2=test[['ds', 'y']].set_index('ds')

perfcounterstop = time.perf_counter()
print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

print(f'MAE : {mean_absolute_error(forecast2,test2)}')
print(f'MSE : {mean_squared_error(forecast2,test2)}')