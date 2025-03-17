from call_API.mf_rework_un_output import get_meteo_france_data_aggregated
from call_API.function_api_nasa import call_api_nasa_yearly  
from call_API.nrj_iteration_avec_output import get_energy_for_all_regions
from preproc.convert_data_energy import convert_energy_data
from preproc.function_merge_flows import consolidation_data
from preproc.function_resampling_MF import resampling_meteo_france_data
from preproc.function_resampling_NASA import conversion_30mins
from preproc.function_merge_region_moy_NASA import merge30mins_regions

if __name__=="__main__":
    get_meteo_france_data_aggregated(2013, 2023)
    call_api_nasa_yearly()
    resampling_meteo_france_data(frequency='30 min',method='interpolate')
    conversion_30mins()
    merge30mins_regions()
    get_energy_for_all_regions()
    convert_energy_data()
    consolidation_data()
