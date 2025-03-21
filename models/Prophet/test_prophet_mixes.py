import pandas as pd
import matplotlib.pyplot as plt
import os
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import time
from config import CSV_PATH

PATH_FILE_INPUT = os.path.join(CSV_PATH, "dataset_final_resampled.csv")

perfcounterstart = time.perf_counter()
df = pd.read_csv(PATH_FILE_INPUT)


# list of the mixes : "thermique","nucleaire","eolien","solaire", "hydraulique","pompage","bioenergies","echanges"
mixes=["thermique","nucleaire","eolien","solaire", "hydraulique","pompage","bioenergies","echanges"]
for mix in mixes :

    periods=90*8
    region="Auvergne-Rhône-Alpes"
    try :
        os.mkdir(os.path.join(CSV_PATH, region))
    except :
        print("Directory stil exists")
    PNG_FOLDER=os.path.join(CSV_PATH, region, str(periods))
    try :
        os.mkdir(PNG_FOLDER)
    except :
        print("Directory stil exists")

    df2_=df[df["region"]==region].copy() # filter on the region

    df2=df2_[["date_timestamp",mix]].copy()
    df2.rename(columns={'date_timestamp':'ds',
                   mix:'y'},inplace=True)
    train = df2[df2["ds"]< "2022-01-01"] # period 2013 - 2021 included
    print("extrait train :")
    print(train.head())
    test = df2[df2["ds"]>= "2022-01-01"] # only period 2022
    print("extrait test :")
    print(test.head())

    m = Prophet()
    m.fit(train)

    future = m.make_future_dataframe(periods=periods,freq="3h", include_history=False)

    forecast = m.predict(future)
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
    forecast2=forecast[['ds', 'yhat']].set_index('ds')
    test2=test[['ds', 'y']].set_index('ds')[:periods]

    perfcounterstop = time.perf_counter()
    print(f"⏰Elapsed time : {perfcounterstop - perfcounterstart:.4} s")

    print(f'MAE : {mean_absolute_error(forecast2,test2)}')
    print(f'MSE : {mean_squared_error(forecast2,test2)}')

    test3=test2.reset_index()
    test3['ds']=pd.to_datetime(test3['ds']) #convert ds to datetime
    test3=test3.set_index(keys='ds')

    plt.plot(forecast['ds'], forecast['yhat'],label="yhat - predict")
    plt.plot(test3.index, test3['y'], label="y - real")
    plt.legend(loc="lower left")
    plt.title(f'{mix} - number of periods : {periods}\nElapsed time : {perfcounterstop - perfcounterstart:.4} s - MAE : {round(mean_absolute_error(forecast2,test2),2)} - MSE : {round(mean_squared_error(forecast2,test2),2)}')
    PATH_FILE_OUTPUT = os.path.join(PNG_FOLDER, mix+" periods "+str(periods)+".png")
    plt.savefig(PATH_FILE_OUTPUT) # save the result to png
    plt.close()