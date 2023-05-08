## extract_multiple_plants_data_to_multiple_csv_worker.py

import os
import pandas as pd
import numpy as np

import xml.etree.ElementTree as ET
import xmltodict

import time
#from datetime import datetime

# Create common namespace
import common_namespace_SG as cn
path_dict = cn.create_path_names_dict()                        # Create pathnames
file_names_dict = cn.create_file_names_dict()                        # Create filenames

def extract_multiple_plants_data_to_multiple_csv_worker(xml_file):
    # Identify prefix_data_type
    registrated_prefix_types = [
        'prefix_solar_unity',
        'prefix_wind_unity',
        'prefix_biomass_unity',
        'prefix_ee_other_unity',
        'prefix_nuclear_generator_unity',
        'prefix_power_consumer_unity',
        'prefix_combustion_unity',
        'prefix_water_generator_unity',
        'prefix_storage_unity',
        'prefix_gas_generator_unity',
        'prefix_gas_storage_unity',
        'prefix_gas_consumer_unity',
        'prefix_grid_Marktakteure_unity',
        'prefix_grid_balancing_area',
        'prefix_grid_single_grid_unity',
        'prefix_grid_roles_unity',
        'prefix_grid_value_unity',
        'prefix_grid_category_unity',
        'prefix_grid_unittypes_unity'
        ]
    prefix_data_type = 'prefix_data_type unknown'
    for registrated_type in registrated_prefix_types:
        if file_names_dict['filenames_raw'][registrated_type] in xml_file:
            prefix_data_type = file_names_dict['filenames_raw'][registrated_type]
 

    # grap relevant files
    available_raw_data_MaStR = os.listdir(path_dict['MaStR_xml'])              # returns list with filenames
    substring_unity_type = prefix_data_type
    available_raw_data_MaStR_unity = []

    for file in available_raw_data_MaStR:       
        if substring_unity_type in file:
            available_raw_data_MaStR_unity.append(file.removesuffix('.xml'))
            #available_raw_data_MaStR_solar.append(file)
    
    number_files_of_this_unity_type = len(available_raw_data_MaStR_unity)
    

    # define function for df templates for each prefix_data_type
    def create_dataframe_unity_templateas_dict(prefix_data_type):
        '''define features to be extracted from MaStR xml files

        for these two prefix_data_type:
            prefix_solar_unity
            prefix_storage_unity
        changes have to be made also in:
            extract_multiple_plants_data_to_multiple_csv_worker.py
        '''
        # create extraction templates
        if prefix_data_type == file_names_dict['filenames_raw']['prefix_solar_unity']:
            # Create template dict for feature extraction
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Postleitzahl' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitSystemstatus' : '',
                'EinheitBetriebsstatus' : '',
                'NameStromerzeugungseinheit' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'ZugeordneteWirkleistungWechselrichter' : '',
                'AnzahlModule' : '',
                'Hauptausrichtung' : '',
                'HauptausrichtungNeigungswinkel' : '',
                'FernsteuerbarkeitNb' : '',
                'Einspeisungsart' : '',
                'Nutzungsbereich' : ''                
            }
        
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_wind_unity']:
            # Create template for feature extraction
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitSystemstatus' : '',
                'EinheitBetriebsstatus' : '',
                'NameStromerzeugungseinheit' : '',
                'NameWindpark' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',
                'Lage' : '',
                'Technologie' : '',
                'Hersteller' : '',
                'Typenbezeichnung' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'Nabenhoehe' : '',
                'Rotordurchmesser' : ''
            }

        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_biomass_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitSystemstatus' : '',
                'EinheitBetriebsstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',
                'NameStromerzeugungseinheit' : '',
                'Energietraeger' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'Einspeisungsart' : '',
                'Hauptbrennstoff' : '',          
                'Biomasseart' : '',
                'Technologie' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_ee_other_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitSystemstatus' : '',
                'EinheitBetriebsstatus' : '',
                'Postleitzahl' : '',
                'NameStromerzeugungseinheit' : '',
                'Energietraeger' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'Einspeisungsart' : '',       
                'Technologie' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_nuclear_generator_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'DatumEndgueltigeStilllegung' : '',
                'EinheitSystemstatus' : '',
                'EinheitBetriebsstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',
                'NameStromerzeugungseinheit' : '',
                'Energietraeger' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'Einspeisungsart' : '',
                'NameKraftwerk' : '',
                'NameKraftwerksblock' : '',                       
                'Technologie' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_power_consumer_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitSystemstatus' : '',
                'EinheitBetriebsstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',                
                'NameStromverbrauchseinheit' : '',
                'AnzahlStromverbrauchseinheitenGroesser50Mw' : '',
                'PraequalifiziertGemaessAblav' : '',
                'AnteilBeinflussbareLast' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_combustion_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitBetriebsstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',                
                'NameStromerzeugungseinheit' : '',
                'Energietraeger' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'Einspeisungsart' : '',
                'NameKraftwerk' : '',
                'NameKraftwerksblock' : '',
                'Hauptbrennstoff' : '',
                'Notstromaggregat' : '',
                'Technologie' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_water_generator_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitBetriebsstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',                
                'NameStromerzeugungseinheit' : '',
                'Energietraeger' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'FernsteuerbarkeitNb' : '',
                'Einspeisungsart' : '',
                'NameKraftwerk' : '',
                'ArtDerWasserkraftanlage' : '',
                'ArtDesZuflusses' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_storage_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitBetriebsstatus' : '',
                'EinheitSystemstatus' : '',
                'Postleitzahl' : '',
                'NameStromerzeugungseinheit' : '',
                'Energietraeger' : '',
                'Bruttoleistung' : '',
                'Nettonennleistung' : '',
                'FernsteuerbarkeitNb' : '',
                'Einspeisungsart' : '',
                'AcDcKoppelung' : '',
                'Batterietechnologie' : '',
                'Notstromaggregat' : '',
                'ZugeordnenteWirkleistungWechselrichter': '',
                'Technologie' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_gas_generator_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitBetriebsstatus' : '',
                'EinheitSystemstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',
                'NameGaserzeugungseinheit' : '',
                'Erzeugungsleistung' : '',
                'Technologie' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_gas_storage_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitBetriebsstatus' : '',
                'EinheitSystemstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',
                'NameGasspeicher' : '',
                'Speicherart' : '',
                'MaximalNutzbaresArbeitsgasvolumen' : '',
                'MaximaleEinspeicherleistung' : '',
                'MaximaleAusspeicherleistung' : '',
                'DurchschnittlicherBrennwert' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_gas_consumer_unity']:
            dataframe_unity_template = {
                'EinheitMastrNummer' : '',
                'LokationMaStRNummer' : '',
                'AnlagenbetreiberMastrNummer' : '',
                'Inbetriebnahmedatum' : '',
                'EinheitBetriebsstatus' : '',
                'EinheitSystemstatus' : '',
                'Postleitzahl' : '',
                'Laengengrad' : '',
                'Breitengrad' : '',
                'NameGasverbrauchsseinheit' : '',
                'MaximaleGasbezugsleistung' : '',
                'EinheitDientDerStromerzeugung' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_grid_Marktakteure_unity']:
            dataframe_unity_template = {
                'MastrNummer' : '',
                'Personenart' : '',
                'Firmenname' : '',
                'Marktfunktion' : '',
                'Rechtsform' : '',
                'Postleitzahl' : '',
                'Taetigkeitsbeginn' : '',
                'Taetigkeitsende_nv' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_grid_balancing_area']:
            pass
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_grid_single_grid_unity']:
            pass
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_grid_roles_unity']:
            dataframe_unity_template = {
                'MarktakteurMastrNummer' : '',
                'MastrNummer' : '',
                'Marktrolle' : '',
                'KontaktdatenMarktrolle' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_grid_value_unity']:
            dataframe_unity_template = {
                'Id' : '',
                'Wert' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_grid_category_unity']:
            dataframe_unity_template = {
                'Id' : '',
                'Name' : ''
            }
        elif prefix_data_type == file_names_dict['filenames_raw']['prefix_grid_unittypes_unity']:
            dataframe_unity_template = {
                'Id' : '',
                'Wert' : ''
            }
        try:
            return dataframe_unity_template
        except:
            pass
    
    
    
    # define specific data cleaning per unity type
    def specific_data_cleaning_single_csvs(prefix_data_type, dataframe_unity):
        '''perform data cleaning operations depending on prefix_data_type

        For the following prefix_data_type:
            file_names_dict['filenames_raw']['prefix_combustion_unity']
            file_names_dict['filenames_raw']['prefix_storage_unity']
        there is a exact copy of this function in:
            extract_data_from_MaStR_from_xml_to_csv(prefix_data_type) in common_functions_SG_2.py and
            extract_multiple_plants_data_to_multiple_csv_worker_2(xml_file) in extract_multiple_plants_data_to_multiple_csv_worker.py
        were the same operations are performed on the single xml_to_csv files in path_dict['single_MaStR_csv']

        '''
        
        # perform the standard cleaning
        standard_str_cleaning_columns = [x for x in file_names_dict['filenames_raw']['list_columns_to_clean_standard']['standard_str_cleaning'] if x in dataframe_unity.columns]
        standard_datetime_cleaning_columns = [x for x in file_names_dict['filenames_raw']['list_columns_to_clean_standard']['standard_datetime_cleaning'] if x in dataframe_unity.columns]
        standard_numeric_cleaning_columns = [x for x in file_names_dict['filenames_raw']['list_columns_to_clean_standard']['standard_numeric_cleaning'] if x in dataframe_unity.columns]

        for c in standard_str_cleaning_columns:
            dataframe_unity[c] = dataframe_unity[c].astype(str)
        for c in standard_datetime_cleaning_columns:
            dataframe_unity[c] = pd.to_datetime(dataframe_unity[c], errors="coerce")
        for c in standard_numeric_cleaning_columns:
            dataframe_unity[c] = pd.to_numeric(dataframe_unity[c], errors="coerce")
          
        
        # identify plants in operating mode
        set_operating_tag = 1
        if set_operating_tag:
            if str('EinheitBetriebsstatus') in dataframe_unity.columns:
                dataframe_unity['operating'] = np.nan
                dataframe_unity['operating'] = dataframe_unity['operating'].mask(dataframe_unity['EinheitBetriebsstatus'] == 35, int(1)).mask(dataframe_unity['EinheitBetriebsstatus'] != 35, int(0))
            

        if prefix_data_type in file_names_dict['filenames_raw']['list_string_columns_to_clean_asci_128'].keys():
            list_of_str_columns = file_names_dict['filenames_raw']['list_string_columns_to_clean_asci_128'][prefix_data_type]
            def remove_non_ascii(string):
                return ''.join(char for char in string if ord(char) < 128)
            for c in list_of_str_columns:
                column_list = list(dataframe_unity[c])
                column_list = [str(x) for x in column_list]
                column_list = [remove_non_ascii(string) for string in column_list]
                dataframe_unity[c] = column_list            

        try:
            return dataframe_unity
        except:
            pass
        # define specific data cleaning per unity type
    
    
    def load_and_transform_xml_file(xml_file):    

        # import xml,

        filename_import = f'{xml_file}.xml'
        raw_data_path_MaStR = path_dict['MaStR_xml']
        xml_path = os.path.realpath(f'{raw_data_path_MaStR}\\{filename_import}')
        tree = ET.parse(xml_path)
        xml_data = tree.getroot()
        xmlstr = ET.tostring(xml_data, encoding='utf-8', method='xml')
        data_dict_unity = dict(xmltodict.parse(xmlstr))

        #print(time.strftime("%H:%M:%S", time.localtime()) + ' : ' + xml_file + ' : xmltodict done')

        # Generate df for data storage strings for nested accessors
        dataframe_unity_template = create_dataframe_unity_templateas_dict(prefix_data_type)
        data_dict_unity_l1 = prefix_data_type
        data_dict_unity_l2 = file_names_dict['filenames_raw']['data_dict_unity_l2_transformation'][data_dict_unity_l1]
        dataframe_unity = pd.DataFrame([dataframe_unity_template])

        # Extact features per row
        for u in range(len(data_dict_unity[data_dict_unity_l1][data_dict_unity_l2])):

            # Generate temporary dict for each loop iteration
            dict_to_dataframe_unity = dataframe_unity_template

            # Extract rows per requested feature in corresponding template
            for feature in list(dataframe_unity_template.keys()):
                dict_to_dataframe_unity[feature] = data_dict_unity[data_dict_unity_l1][data_dict_unity_l2][u].get(feature)
            
            # transform dict to df
            dict_to_dataframe_unity = pd.DataFrame([dict_to_dataframe_unity])

            # add data to big df
            dataframe_unity = pd.concat([dataframe_unity, dict_to_dataframe_unity], axis=0, ignore_index=False)

        # perform data cleaning beforew writing single csv files
        dataframe_unity = specific_data_cleaning_single_csvs(prefix_data_type, dataframe_unity)

        # drop first line in df (due to creation from empty df)
        dataframe_unity = dataframe_unity.iloc[1: ,:]

        if number_files_of_this_unity_type == 1:
            # save CSV in 02_csv_data_static_files
            try:
                filename_export_part_1 = prefix_data_type
                filename_export_part_2 = file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix']
                filename_export = f'{filename_export_part_1}{filename_export_part_2}'
                os.chdir(path_dict['static_input'])
                dataframe_unity.to_csv(filename_export)
                #print(time.strftime("%H:%M:%S", time.localtime()) + ' : ' + xml_file + ' : feature extraction done and file saved')
            except:
                print(time.strftime("%H:%M:%S", time.localtime()) + ' : ' + xml_file + ' : feature extraction done but saving file crashed')
        elif number_files_of_this_unity_type > 1:
            # save CSV in 02_csv_data_static_files\\_single_CSV
            try:
                filename_export = f'{xml_file}.csv'
                os.chdir(path_dict['single_MaStR_csv'])
                dataframe_unity.to_csv(filename_export)
                #print(time.strftime("%H:%M:%S", time.localtime()) + ' : ' + xml_file + ' : feature extraction done and file saved')
            except:
                print(time.strftime("%H:%M:%S", time.localtime()) + ' : ' + xml_file + ' : feature extraction done but saving file crashed')

    load_and_transform_xml_file(xml_file)

    try:
        return xml_file
    except:
        pass
