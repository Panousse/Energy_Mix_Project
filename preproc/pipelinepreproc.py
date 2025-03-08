from call_API.function_API_energy import get_energy_data_from_web
from call_API.function_API_MF import get_meteo_france_data_from_web
from preproc.convert_data_energy import convert_energy_data
from preproc.function_aggregating_MF import agregating_meteo_france_data
from preproc.function_merge_flows import consolidation_data
from preproc.function_resampling_MF import resampling_meteo_france_data

if __name__=="__main__":
    get_meteo_france_data_from_web(2013, 2023)
    agregating_meteo_france_data()
    resampling_meteo_france_data(frequency='30 min',method='interpolate')
    get_energy_data_from_web()
    convert_energy_data()
    consolidation_data()
