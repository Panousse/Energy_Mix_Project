from call_API.function_API_energy import get_energy_data_from_web
from call_API.function_API_MF import get_meteo_france_data_from_web
from call_API.function_api_nasa import call_api_nasa_yearly
from preproc.convert_data_energy import convert_energy_data
from preproc.function_aggregating_MF import agregating_meteo_france_data
from preproc.function_merge_flows import consolidation_data
from preproc.function_resampling_MF import resampling_meteo_france_data
from preproc.function_resampling_NASA import conversion_30mins
from preproc.function_merge_region_moy_NASA import merge30mins_regions

if __name__=="__main__":
    get_meteo_france_data_from_web(2013, 2023)
    agregating_meteo_france_data()
    call_api_nasa_yearly()
    resampling_meteo_france_data(frequency='30 min',method='interpolate')
    conversion_30mins()
    merge30mins_regions()
    get_energy_data_from_web()
    convert_energy_data()
    consolidation_data()
