import pandas as pd
from prophet import Prophet
import os
from config import CSV_PATH
energy_input="dataset_final.csv"
PATH_ENERGY = os.path.join(CSV_PATH, energy_input)
df = pd.read_csv(PATH_ENERGY)
df2=df.copy()
df2["production_totale"]=df['thermique']+df['nucleaire']+df['eolien']+df['solaire']+df['hydraulique']+df['pompage']+df['bioenergies']
df2.drop(columns=["region","consommation","thermique","nucleaire","eolien","solaire", "hydraulique","pompage","bioenergies","echanges","vitesse_vent","temperature","valeur"],axis=1,inplace=True)
df2.rename(columns={'date_timestamp':'ds',
                   'production_totale':'y'},inplace=True)
print(df2.head())
m = Prophet()
m.fit(df2)

future = m.make_future_dataframe(periods=365*48,freq="30min", include_history="False")
print(future.tail())

forecast = m.predict(future)
forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()
