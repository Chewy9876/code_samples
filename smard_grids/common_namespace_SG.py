# common_namespace_SG.py

# import all used packages
import os
from os.path import exists
import numpy as np
from datetime import datetime


# Create path_dict

def create_path_names_dict():
    path_names_dict = {
        'rel_path_l0_script_files' : 'code_samples',
        'rel_path_l0_data_lake' : 'code_sample_data',

        'rel_path_l1_project_name' : 'smart_grids',

        # level 2 folders
        'rel_path_l2_raw_data' : '01_raw_data',
        'rel_path_l2_csv_static_data' : '02_csv_data_static_files',
        'rel_path_l2_csv_dynamic_data' : '03_csv_data_dynamic_files',
        'rel_path_l2_preprocessed_time_series_data' : '10_preproccessed_time_series_data',
        'rel_path_l2_aggregates_storage' : '20_aggregates_storage',
        'rel_path_l2_geodata' : '30_geodata_Germany',
        'rel_path_l2_flow_control_files' : '60_flow_control_files',

        # level 3 folders 01_raw_data
        #'rel_path_l3_MaStR_xml' : "Gesamtdatenexport_20221114_1.2_db36d3a1e4ec400fb82776daa08468ec",       # old path
        'rel_path_l3_MaStR_xml' : "Gesamtdatenauszug_MaStR",
        'rel_path_l3_smard_historical_data' : 'smard_historical_data',
        #'raw_data_path_historical_smard' : "c:\\Users\\chris\\Bootcamp_raw_data\\20_final_project\\01_raw_data\\smard_historical_data\\",
        'rel_path_l3_postcodes_geodata' : "postcodes_geodata_Germany",
        'rel_path_l3_global_irradiation' : "global_irradiation",

        # level 3 folders 02_csv_data_static_files
        'rel_path_l3_MaStR_csv' : "_single_CSV",
        'rel_path_l3_single_weather_per_postcode_csv' : "historical_weather_per_postcode",

        # level 3 folders 03_csv_data_dynamic_files

        # level 3 folders 10_preproccessed_time_series_data

        # level 3 folders 20_aggregates_storage

    }

    #project_folder_name = os.getcwd().split('\\')[-1:][0]
    global path_dict
    path_dict = {}
    
    # level 1 folders for specific project
    ## add here the path to the data of the repository:
    path_dict['work_path'] = 'D:\\Coding\\code_samples\\smard_grids'
    #path_dict['work_path'] = os.path.realpath(os.getcwd())
    path_dict['data_lake_path'] = os.path.realpath(os.path.join(path_dict['work_path'], '..', '..', path_names_dict['rel_path_l0_data_lake'], path_names_dict['rel_path_l1_project_name']))

    # raw data for specific project
    path_dict['MaStR_xml'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_raw_data'], path_names_dict['rel_path_l3_MaStR_xml']))
    path_dict['smard_historical_data'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_raw_data'], path_names_dict['rel_path_l3_smard_historical_data']))
    path_dict['postcodes_geodata'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_raw_data'], path_names_dict['rel_path_l3_postcodes_geodata']))
    path_dict['global_irradiation'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_raw_data'], path_names_dict['rel_path_l3_global_irradiation']))

    # preprocessed data static
    path_dict['static_input'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_csv_static_data']))
    path_dict['single_MaStR_csv'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_csv_static_data'], path_names_dict['rel_path_l3_MaStR_csv']))
    path_dict['weather_per_postcode_csv'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_csv_static_data'], path_names_dict['rel_path_l3_single_weather_per_postcode_csv']))

    # preprocessed data dynamic
    path_dict['dynamic_input'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_csv_dynamic_data']))
    
    # further folders
    path_dict['preprocessed_time_series'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_preprocessed_time_series_data']))
    path_dict['aggregates_storage'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_aggregates_storage']))
    path_dict['geodata'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_geodata']))
    path_dict['flow_control_files'] = os.path.realpath(os.path.join(path_dict['data_lake_path'], path_names_dict['rel_path_l2_flow_control_files']))

    try:
        return path_dict
    except NameError:
        pass

    

# Creation of filename lists for import

def create_file_names_dict():

    ## MaStR xml file preproccessing
    global file_names_dict
    file_names_dict = {
        'filenames_raw' : {},
        'filenames_csv_exports_MaStR' : {},
        'filenames_aggregates_storage' : {},
        'filenames_raw_historical_Smard' : {},
        'filenames_csv_exports_historical_Smard' : {},
        'filenames_raw_postcodes_geodata' : {},
        'filenames_csv_postcodes_geodata' : {},
        'filenames_csv_postcodes_weatherdata' : {},
        'filename_specs' : {}
    }


    number_AnlagenEegSolar = 27
    number_EinheitenSolar = 28
    number_Marktakteure = 24
    number_AnlagenStromSpeicher = 6
    number_Lokationen = 30
    number_Netzanschlusspunkte = 25

    def build_AnlagenEegSolar_filename_list(number_AnlagenEegSolar):
        filename_list = []
        for i in range(1, number_AnlagenEegSolar+1):
            filename_list.append(f'AnlagenEegSolar_{i}.xml')
        return filename_list

    def build_EinheitenSolar_filename_list(number_EinheitenSolar):
        filename_list = []
        for i in range(1, number_EinheitenSolar+1):
            filename_list.append(f'EinheitenSolar_{i}.xml')
        return filename_list

    def build_Marktakteure_filename_list(number_Marktakteure):
        filename_list = []
        for i in range(1, number_Marktakteure+1):
            filename_list.append(f'Marktakteure_{i}.xml')
        return filename_list

    def build_AnlagenStromSpeicher_filename_list(number_AnlagenStromSpeicher):
        filename_list = []
        for i in range(1, number_AnlagenStromSpeicher+1):
            filename_list.append(f'AnlagenStromSpeicher_{i}.xml')
        return filename_list

    def build_Lokationen_filename_list(number_Lokationen):
        filename_list = []
        for i in range(1, number_Lokationen+1):
            filename_list.append(f'Lokationen_{i}.xml')
        return filename_list

    def build_Netzanschlusspunkte_filename_list(number_Netzanschlusspunkte):
        filename_list = []
        for i in range(1, number_Netzanschlusspunkte+1):
            filename_list.append(f'Netzanschlusspunkte_{i}.xml')
        return filename_list

    # Create filename_lists for import
    file_names_dict['filenames_raw']['AnlagenEegSolar_list'] = build_AnlagenEegSolar_filename_list(number_AnlagenEegSolar)
    file_names_dict['filenames_raw']['EinheitenSolar_list'] = build_EinheitenSolar_filename_list(number_EinheitenSolar)
    file_names_dict['filenames_raw']['Marktakteure_list'] = build_Marktakteure_filename_list(number_Marktakteure)
    file_names_dict['filenames_raw']['AnlagenStromSpeicher_list'] = build_AnlagenStromSpeicher_filename_list(number_AnlagenStromSpeicher)
    file_names_dict['filenames_raw']['Lokationen_list'] = build_Lokationen_filename_list(number_Lokationen)
    file_names_dict['filenames_raw']['Netzanschlusspunkte_list'] = build_Netzanschlusspunkte_filename_list(number_Netzanschlusspunkte)

    # Create single filenames for import
    file_names_dict['filenames_raw']['AnlagenEegBiomasse'] = 'AnlagenEegBiomasse.xml'
    file_names_dict['filenames_raw']['AnlagenEegGeoSolarthermieGrubenKlaerschlammDruckentspannung'] = 'AnlagenEegGeoSolarthermieGrubenKlaerschlammDruckentspannung.xml'
    file_names_dict['filenames_raw']['AnlagenEegWasser'] = 'AnlagenEegWasser.xml'
    file_names_dict['filenames_raw']['AnlagenEegWind'] = 'AnlagenEegWind.xml'
    file_names_dict['filenames_raw']['AnlagenGasSpeicher'] = 'AnlagenGasSpeicher.xml'
    file_names_dict['filenames_raw']['AnlagenKwk'] = 'AnlagenKwk.xml'
    file_names_dict['filenames_raw']['Bilanzierungsgebiete'] = 'Bilanzierungsgebiete.xml'
    file_names_dict['filenames_raw']['EinheitenBiomasse'] = 'EinheitenBiomasse.xml'
    file_names_dict['filenames_raw']['EinheitenGasErzeuger'] = 'EinheitenGasErzeuger.xml'
    file_names_dict['filenames_raw']['EinheitenGasSpeicher'] = 'EinheitenGasSpeicher.xml'
    file_names_dict['filenames_raw']['EinheitenGasverbraucher'] = 'EinheitenGasverbraucher.xml'
    file_names_dict['filenames_raw']['EinheitenGenehmigung'] = 'EinheitenGenehmigung.xml'
    file_names_dict['filenames_raw']['EinheitenGeoSolarthermieGrubenKlaerschlammDruckentspannung'] = 'EinheitenGeoSolarthermieGrubenKlaerschlammDruckentspannung.xml'
    file_names_dict['filenames_raw']['EinheitenKernkraft'] = 'EinheitenKernkraft.xml'
    file_names_dict['filenames_raw']['EinheitenStromVerbraucher'] = 'EinheitenStromVerbraucher.xml'
    file_names_dict['filenames_raw']['Einheitentypen'] = 'Einheitentypen.xml'
    file_names_dict['filenames_raw']['EinheitenVerbrennung'] = 'EinheitenVerbrennung.xml'
    file_names_dict['filenames_raw']['EinheitenWasser'] = 'EinheitenWasser.xml'
    file_names_dict['filenames_raw']['EinheitenWind'] = 'EinheitenWind.xml'
    file_names_dict['filenames_raw']['GeloeschteUndDeaktivierteEinheiten'] = 'GeloeschteUndDeaktivierteEinheiten.xml'
    file_names_dict['filenames_raw']['Katalogkategorien'] = 'Katalogkategorien.xml'
    file_names_dict['filenames_raw']['Katalogwerte'] = 'Katalogwerte.xml'
    file_names_dict['filenames_raw']['Marktrollen'] = 'Marktrollen.xml'
    file_names_dict['filenames_raw']['Netze'] = 'Netze.xml'
    file_names_dict['filenames_raw']['gloabl_irradiation_ffm'] = 'Timeseries_50.123_8.644_SA2_35deg_0deg_2005_2019.csv'

    # MaStR raw filename decomposition
    ## Power unities
    file_names_dict['filenames_raw']['delimiter_multiple_xml_files'] = '_'
    file_names_dict['filenames_raw']['prefix_solar_unity'] = 'EinheitenSolar'
    file_names_dict['filenames_raw']['prefix_wind_unity'] = 'EinheitenWind'
    file_names_dict['filenames_raw']['prefix_biomass_unity'] = 'EinheitenBiomasse'
    file_names_dict['filenames_raw']['prefix_ee_other_unity'] = 'EinheitenGeoSolarthermieGrubenKlaerschlammDruckentspannung'
    file_names_dict['filenames_raw']['prefix_nuclear_generator_unity'] = 'EinheitenKernkraft'
    file_names_dict['filenames_raw']['prefix_power_consumer_unity'] = 'EinheitenStromVerbraucher'
    file_names_dict['filenames_raw']['prefix_combustion_unity'] = 'EinheitenVerbrennung'
    file_names_dict['filenames_raw']['prefix_water_generator_unity'] = 'EinheitenWasser'
    file_names_dict['filenames_raw']['prefix_storage_unity'] = 'EinheitenStromSpeicher'

    ## Gas unities
    file_names_dict['filenames_raw']['prefix_gas_generator_unity'] = 'EinheitenGasErzeuger'
    file_names_dict['filenames_raw']['prefix_gas_storage_unity'] = 'EinheitenGasSpeicher'
    file_names_dict['filenames_raw']['prefix_gas_consumer_unity'] = 'EinheitenGasverbraucher'

    ## grid unities
    file_names_dict['filenames_raw']['prefix_grid_MaLo_unity'] = 'Lokationen'
    file_names_dict['filenames_raw']['prefix_grid_Marktakteure_unity'] = 'Marktakteure'
    file_names_dict['filenames_raw']['prefix_grid_Netzanschlusspunkte_unity'] = 'Netzanschlusspunkte'
    file_names_dict['filenames_raw']['prefix_grid_balancing_area'] = 'Bilanzierungsgebiete'
    file_names_dict['filenames_raw']['prefix_grid_single_grid_unity'] = 'Netze'
    file_names_dict['filenames_raw']['prefix_grid_roles_unity'] = 'Marktrollen'
    file_names_dict['filenames_raw']['prefix_grid_value_unity'] = 'Katalogwerte'
    file_names_dict['filenames_raw']['prefix_grid_category_unity'] = 'Katalogkategorien'
    file_names_dict['filenames_raw']['prefix_grid_unittypes_unity'] = 'Einheitentypen'

    ## transform xml level accessors from plural to singular
    file_names_dict['filenames_raw']['data_dict_unity_l2_transformation'] = {
        'EinheitenSolar': 'EinheitSolar',
        'EinheitenWind': 'EinheitWind',
        'EinheitenBiomasse': 'EinheitBiomasse',
        'EinheitenGeoSolarthermieGrubenKlaerschlammDruckentspannung': 'EinheitGeoSolarthermieGrubenKlaerschlammDruckentspannung',
        'EinheitenStromVerbraucher': 'EinheitStromVerbraucher',
        'EinheitenKernkraft': 'EinheitKernkraft',
        'EinheitenVerbrennung': 'EinheitVerbrennung',
        'EinheitenWasser': 'EinheitWasser',
        'EinheitenStromSpeicher': 'EinheitStromSpeicher',
        'EinheitenGasErzeuger': 'EinheitGasErzeuger',
        'EinheitenGasSpeicher': 'EinheitGasSpeicher',
        'EinheitenGasverbraucher': 'EinheitGasverbraucher',
        'Marktakteure': 'Marktakteur',
        'Bilanzierungsgebiete': 'Bilanzierungsgebiet',
        'Netze': 'Netz',
        'Marktrollen': 'Marktrolle',
        'Katalogwerte' : 'Katalogwert',
        'Katalogkategorien' : 'Katalogkategorie',
        'Einheitentypen' : 'Einheitentyp'
    }

    ## define the string columns, which shall be cleaned in asci 128 manner
    file_names_dict['filenames_raw']['list_columns_to_clean_standard'] = {
        'standard_str_cleaning': ['EinheitMastrNummer', 'LokationMaStRNummer', 'AnlagenbetreiberMastrNummer', 'MastrNummer'],
        'standard_datetime_cleaning': ['Inbetriebnahmedatum', 'Taetigkeitsbeginn', 'DatumEndgueltigeStilllegung'],
        'standard_numeric_cleaning': ['EinheitBetriebsstatus']
    }

    ## define the string columns, which shall be cleaned in asci 128 manner
    file_names_dict['filenames_raw']['list_string_columns_to_clean_asci_128'] = {
        'EinheitenSolar': ['NameStromerzeugungseinheit'],
        'EinheitenWind': ['NameStromerzeugungseinheit', 'NameWindpark', 'Typenbezeichnung'],
        'EinheitenBiomasse': ['NameStromerzeugungseinheit'],
        'EinheitenGeoSolarthermieGrubenKlaerschlammDruckentspannung': ['NameStromerzeugungseinheit'],
        'EinheitenStromVerbraucher': ['NameStromverbrauchseinheit'],
        'EinheitenKernkraft': ['NameStromerzeugungseinheit', 'NameKraftwerk', 'NameKraftwerksblock'],
        'EinheitenVerbrennung': ['NameStromerzeugungseinheit', 'NameKraftwerk', 'NameKraftwerksblock'],
        'EinheitenWasser': ['NameStromerzeugungseinheit', 'NameKraftwerk'],
        'EinheitenStromSpeicher': ['NameStromerzeugungseinheit'],
        'EinheitenGasErzeuger': ['NameGaserzeugungseinheit'],
        'EinheitenGasSpeicher': ['NameGasspeicher'],
        'EinheitenGasverbraucher': ['NameGasverbrauchsseinheit'],
        'Marktakteure': ['Firmenname'],
        'Bilanzierungsgebiete': [],
        'Netze': [],
        'Marktrollen': ['KontaktdatenMarktrolle'],
        'Katalogwerte' : [],
        'Katalogkategorien' : [],
        'Einheitentypen' : []
    }

    ## define the string columns, where the UID of the operators are stored
    file_names_dict['filenames_raw']['column_names_operator'] = {
        'EinheitenSolar': 'AnlagenbetreiberMastrNummer',
        'EinheitenWind': 'AnlagenbetreiberMastrNummer',
        'EinheitenBiomasse': 'AnlagenbetreiberMastrNummer',
        'EinheitenGeoSolarthermieGrubenKlaerschlammDruckentspannung': 'AnlagenbetreiberMastrNummer',
        'EinheitenStromVerbraucher': 'AnlagenbetreiberMastrNummer',
        'EinheitenKernkraft': 'AnlagenbetreiberMastrNummer',
        'EinheitenVerbrennung': 'AnlagenbetreiberMastrNummer',
        'EinheitenWasser': 'AnlagenbetreiberMastrNummer',
        'EinheitenStromSpeicher': 'AnlagenbetreiberMastrNummer',
        'EinheitenGasErzeuger': 'AnlagenbetreiberMastrNummer',
        'EinheitenGasSpeicher': 'AnlagenbetreiberMastrNummer',
        'EinheitenGasverbraucher': 'AnlagenbetreiberMastrNummer',
        # Marktakteure is the table with the UID, joining column 'Firma', has to be the last key in this dict
        'Marktakteure': ['MastrNummer', 'AnlagenbetreiberMastrNummer'],
    }

    file_names_dict['filenames_raw']['column_names_operator_clear_name_marktakteure'] = 'Firmenname'

    # Create filenames for export
    file_names_dict['filenames_csv_exports_MaStR']['dataframe_wind_file_name_prefix'] = f'wind_df.csv'
    file_names_dict['filenames_csv_exports_MaStR']['dataframe_solar_file_name_prefix'] = f'solar_df.csv'
    file_names_dict['filenames_csv_exports_MaStR']['dataframe_solar_file_name_sufix'] = f'.csv'
    file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix'] = f'_df.csv'
    file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix_with_operator'] = f'_df_operator_joined.csv'

    # Create filenames for aggregates (JSON)
    file_names_dict['filenames_aggregates_storage']['wind_plants_aggregates'] = f'wind_plants_aggregates.json'
    file_names_dict['filenames_aggregates_storage']['solar_plants_aggregates'] = f'solar_plants_aggregates.json'
    file_names_dict['filenames_aggregates_storage']['weather_data_imputation_sufix'] = f'weather_data_imputation.json'
    

    ## smard historical data
    # import
    file_names_dict['filenames_raw_historical_Smard']['generation_2021'] = f'Realisierte_Erzeugung_202101010000_202112312359.csv'
    file_names_dict['filenames_raw_historical_Smard']['generation_2022'] = f'Realisierte_Erzeugung_202201010000_202211232359.csv'
    file_names_dict['filenames_raw_historical_Smard']['consumption_2021'] = f'Realisierter_Stromverbrauch_202101010000_202112312359.csv'
    file_names_dict['filenames_raw_historical_Smard']['consumption_2022'] = f'Realisierter_Stromverbrauch_202201010000_202211232359.csv'    

    # export
    file_names_dict['filenames_csv_exports_historical_Smard']['consumption'] = f'consumption_df.csv'
    file_names_dict['filenames_csv_exports_historical_Smard']['generation'] = f'generation_df.csv'
    file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv'] = f'_df.csv'
    file_names_dict['filenames_csv_exports_historical_Smard']['filename_merged_csv_gen_cons'] = f'smard_gen_cons_df.csv'
    file_names_dict['filenames_csv_exports_historical_Smard']['filename_merged_csv_installed_gen'] = f'smard_installed_gen_df.csv'

    # specifications for historical data smard.de generation and consumption
    file_names_dict['filename_specs'] = {
            'prefix_installed_power' : 'Installierte_Erzeugungsleistung',
            'prefix_measured_generation' : 'Realisierte_Erzeugung',
            'prefix_predicted_generation' : 'Prognostizierte_Erzeugung',
            'prefix_measured_consumption' : 'Realisierter_Stromverbrauch',
            'prefix_predicted_consumption' : 'Prognostizierter_Stromverbrauch',
            'sufix_day' : 'Tag',
            'sufix_15_min' : 'Viertelstunde',
            'delimiter' : '_',
            'sufix_file' : '.csv',
            'archiv_folder' : '_archiv',
            'dtypes_predicted_generation' : {
                'Gesamt [MWh] Berechnete Auflösungen' : np.float64,
                'Photovoltaik und Wind [MWh] Originalauflösungen' : np.float64,
                'Wind Offshore [MWh] Originalauflösungen' : np.float64,
                'Wind Onshore [MWh] Originalauflösungen' : np.float64,
                'Photovoltaik [MWh] Originalauflösungen' : np.float64,
                'Sonstige [MWh] Berechnete Auflösungen' : np.float64
            },
            'na_values_predicted_generation' : ['', '-'],
            'dtypes_measured_generation' : {
                'Biomasse [MWh] Originalauflösungen' : np.float64,
                'Wasserkraft [MWh] Originalauflösungen' : np.float64,
                'Wind Offshore [MWh] Originalauflösungen' : np.float64,
                'Wind Onshore [MWh] Originalauflösungen' : np.float64,
                'Photovoltaik [MWh] Originalauflösungen' : np.float64,
                'Sonstige Erneuerbare [MWh] Originalauflösungen' : np.float64,
                'Kernenergie [MWh] Originalauflösungen' : np.float64,
                'Braunkohle [MWh] Originalauflösungen' : np.float64,
                'Steinkohle [MWh] Originalauflösungen' : np.float64,
                'Erdgas [MWh] Originalauflösungen' : np.float64,
                'Pumpspeicher [MWh] Originalauflösungen' : np.float64,
                'Sonstige Konventionelle [MWh] Originalauflösungen' : np.float64
            },
            'na_values_measured_generation' : ['', '-'],
            'dtypes_predicted_consumption' : {
                'Gesamt (Netzlast) [MWh] Originalauflösungen' : np.float64,
                'Residuallast [MWh] Originalauflösungen' : np.float64
            },
            'na_values_predicted_consumption' : ['', '-'],
            'dtypes_measured_consumption' : {
                'Gesamt (Netzlast) [MWh] Originalauflösungen' : np.float64,
                'Residuallast [MWh] Originalauflösungen' : np.float64,
                'Pumpspeicher [MWh] Originalauflösungen' : np.float64
            },
            'na_values_measured_consumption' : ['', '-'],
            'dtypes_measured_generation_power' : {
                'Biomasse [MW]' : np.float64,
                'Wasserkraft [MW]' : np.float64,
                'Wind Offshore [MW]' : np.float64,
                'Wind Onshore [MW]' : np.float64,
                'Photovoltaik [MW]' : np.float64,
                'Sonstige Erneuerbare [MW]' : np.float64,
                'Kernenergie [MW]' : np.float64,
                'Braunkohle [MW]' : np.float64,
                'Steinkohle [MW]' : np.float64,
                'Erdgas [MW]' : np.float64,
                'Pumpspeicher [MW]' : np.float64,
                'Sonstige Konventionelle [MW]' : np.float64
            },
            'na_values_measured_generation_power' : ['', '-'],
            'column_label_transformation_aggregation' : {
                'Gesamt_(Netzlast)_[MW]': 'Gesamt_(Netzlast)_[MWh]',
                'Residuallast_[MW]' : 'Residuallast_[MWh]',
                'Pumpspeicher_[MW]' : 'Pumpspeicher_[MWh]',
                'Biomasse_[MW]' : 'Biomasse_[MWh]',
                'Wasserkraft_[MW]' : 'Wasserkraft_[MWh]',
                'Wind_Offshore_[MW]' : 'Wind_Offshore_[MWh]',
                'Wind_Onshore_[MW]' : 'Wind_Onshore_[MWh]',
                'Photovoltaik_[MW]' : 'Photovoltaik_[MWh]',
                'Sonstige_Erneuerbare_[MW]' : 'Sonstige_Erneuerbare_[MWh]',
                'Kernenergie_[MW]' : 'Kernenergie_[MWh]',
                'Braunkohle_[MW]' : 'Braunkohle_[MWh]',
                'Steinkohle_[MW]' : 'Steinkohle_[MWh]',
                'Erdgas_[MW]' : 'Erdgas_[MW]',
                'Pumpspeicher_[MW]' : 'Pumpspeicher_[MWh]',
                'Sonstige_Konventionelle_[MW]' : 'Sonstige_Konventionelle_[MWh]'
            },
            'column_label_transformation_aggregation_installed_power' : {
                file_names_dict['filenames_raw']['prefix_biomass_unity'] : 'Biomasse_[MW]',
                file_names_dict['filenames_raw']['prefix_water_generator_unity'] : 'Wasserkraft_[MW]',
                file_names_dict['filenames_raw']['prefix_wind_unity'] : ['Wind_Offshore_[MW]', 'Wind_Onshore_[MW]'],
                file_names_dict['filenames_raw']['prefix_solar_unity'] : 'Photovoltaik_[MW]',
                file_names_dict['filenames_raw']['prefix_ee_other_unity'] : 'Sonstige_Erneuerbare_[MW]',
                file_names_dict['filenames_raw']['prefix_nuclear_generator_unity'] : 'Kernenergie_[MW]',
            },
        }



    ## postcodes geodata Germany
    # import
    file_names_dict['filenames_raw_postcodes_geodata']['postcode_list'] = f'de_postal_codes.csv'
    
    # export
    file_names_dict['filenames_csv_postcodes_geodata']['solar_agg_geodata'] = f'solar_agg_geodata.csv'
    file_names_dict['filenames_csv_postcodes_geodata']['wind_agg_geodata'] = f'wind_agg_geodata.csv'
    file_names_dict['filenames_csv_postcodes_geodata']['fetch_weatherdata'] = f'fetch_weatherdata.csv'

    ## weather data
    # export
    file_names_dict['filenames_csv_postcodes_weatherdata']['temp_per_postcode'] = f'temp_per_postcode.csv'
    file_names_dict['filenames_csv_postcodes_weatherdata']['wspd_per_postcode'] = f'wspd_per_postcode.csv'
    file_names_dict['filenames_csv_postcodes_weatherdata']['tsun_per_postcode'] = f'tsun_per_postcode.csv'
    file_names_dict['filenames_csv_postcodes_weatherdata']['feature_per_postcode_imputed_sufix'] = '_per_postcode_imputed.csv.xz'

    # export json files
    file_names_dict['filenames_csv_postcodes_weatherdata']['verify_imputation_of_all_features'] = f'verify_imputation_of_all_features.json'

    try:
        return file_names_dict
    except NameError:
      pass


# Creation of dict for model specs

def create_model_dict():

    ## MaStR xml file preproccessing
    global model_dict
    model_dict = {
        'leap_years' : {
                '2015' : {
                'switch_year' : False,
                'spring' : datetime(2015, 3, 29, 2, 0),
                'autum' : datetime(2015, 10, 25, 3, 0),
            },
            '2016' : {
                'switch_year' : True,
                'spring' : datetime(2016, 3, 27, 2, 0),
                'autum' : datetime(2016, 10, 30, 3, 0),
            },
            '2017' : {
                'switch_year' : False,
                'spring' : datetime(2017, 3, 26, 2, 0),
                'autum' : datetime(2017, 10, 29, 3, 0),
            },
            '2018' : {
                'switch_year' : False,
                'spring' : datetime(2018, 3, 25, 2, 0),
                'autum' : datetime(2018, 10, 28, 3, 0),
            },
            '2019' : {
                'switch_year' : False,
                'spring' : datetime(2019, 3, 31, 2, 0),
                'autum' : datetime(2019, 10, 27, 3, 0),
            },
            '2020' : {
                'switch_year' : True,
                'spring' : datetime(2020, 3, 29, 2, 0),
                'autum' : datetime(2020, 10, 25, 3, 0),
            },
            '2021' : {
                'switch_year' : False,
                'spring' : datetime(2021, 3, 28, 2, 0),
                'autum' : datetime(2021, 10, 31, 3, 0),
            },
            '2022' : {
                'switch_year' : False,
                'spring' : datetime(2022, 3, 27, 2, 0),
                'autum' : datetime(2022, 10, 30, 3, 0),
            },
            '2023' : {
                'switch_year' : False,
                'spring' : datetime(2023, 3, 26, 2, 0),
                'autum' : datetime(2023, 10, 29, 3, 0),
            }
        },
        'color_of_energy' : {
            'Biomasse' : '#51b351',
            'Wasserkraft' : '#a6e2ff',
            'Wind_Offshore' : '#00b8f2',
            'Wind_Onshore' : '#5a67ff',
            'Photovoltaik' : '#fff340',
            'Sonstige_Erneuerbare' : '#87cd5c',
            'Kernenergie' : '#8d4d4d',
            'Braunkohle' : '#9a7b5c',
            'Steinkohle' : '#414141',
            'Erdgas' : '#c7dada',
            'Pumpspeicher' : '#435474',
            'Sonstige_Konventionelle' : '#758075',
            'Netzlast' : '#ff40000'
        },
        'aggregation_feature_per_postcode_classes_for_installed_power' : {
                file_names_dict['filenames_raw']['prefix_biomass_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                file_names_dict['filenames_raw']['prefix_water_generator_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                file_names_dict['filenames_raw']['prefix_wind_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                file_names_dict['filenames_raw']['prefix_solar_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                #file_names_dict['filenames_raw']['prefix_ee_other_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                file_names_dict['filenames_raw']['prefix_nuclear_generator_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                file_names_dict['filenames_raw']['prefix_power_consumer_unity'] : ['AnzahlStromverbrauchseinheitenGroesser50Mw', 'AnzahlStromverbrauchseinheitenGroesser50Mw'],
                file_names_dict['filenames_raw']['prefix_combustion_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                file_names_dict['filenames_raw']['prefix_storage_unity'] : ['Nettonennleistung', 'Bruttoleistung'],
                file_names_dict['filenames_raw']['prefix_gas_generator_unity'] : ['Erzeugungsleistung', 'Erzeugungsleistung'],
                file_names_dict['filenames_raw']['prefix_gas_storage_unity'] : ['MaximalNutzbaresArbeitsgasvolumen', 'MaximaleAusspeicherleistung'],
                file_names_dict['filenames_raw']['prefix_gas_consumer_unity'] : ['MaximaleGasbezugsleistung', 'EinheitDientDerStromerzeugung'],
                file_names_dict['filenames_raw']['prefix_grid_Marktakteure_unity'] : ['Nettonennleistung', 'Bruttoleistung'],

            },
    }

    
    try:
        return model_dict
    except NameError:
      pass

    


path_dict = create_path_names_dict()                        # Create pathnames
file_names_dict = create_file_names_dict()                        # Create filenames
model_dict = create_model_dict()
