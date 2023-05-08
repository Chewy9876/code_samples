# common_functions_SG.py

# import all used packages
import numpy as np
import pandas as pd
import os
from os.path import exists
import json
import pyproj
import time
from datetime import datetime
from datetime import timedelta

import xml.etree.ElementTree as ET
import xmltodict

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

import seaborn as sns
import seaborn.objects as so

#!pip install geopy
from geopy import distance
import math
#!pip install geopandas
import geopandas

# Import Meteostat library and dependencies
import matplotlib.pyplot as plt
#!pip install meteostat
from meteostat import Point, Daily, Hourly

# Parallelization
#!pip install tqdm
from tqdm import tqdm

#!pip install multiprocess
from multiprocess import Pool
#from magic_functions import process_frame

# function name has to be different from module name
from extract_multiple_plants_data_to_multiple_csv_worker import extract_multiple_plants_data_to_multiple_csv_worker

# Create common namespace
import common_namespace_SG as cn
path_dict = cn.create_path_names_dict()                        # Create pathnames
file_names_dict = cn.create_file_names_dict()                        # Create filenames
model_dict = cn.create_model_dict()


# Check, that all folders exists, if not, create empty folders

def check_folder_struture(path_dict):
    for i in list(path_dict.keys()):
        if os.path.isdir(path_dict[i]):
            pass
            #print(str(i) + " : exists")
        else:
            os.makedirs(path_dict[i])
            #print(str(i) + " : was created")
            #print(str(path_dict[i]) + " : doesn't exists")
        
# Create control flow file loading function
def create_control_flow_mgm_file():
    control_flow_mgm = {
            'preproccessed_raw_files_MaStR' : {}
    }

    try:
        return control_flow_mgm
    except NameError:
      pass

def load_control_flow_file():
    os.chdir(path_dict['flow_control_files'])
    if exists("control_flow_mgm.json"):
        control_flow_mgm = json.load(open("control_flow_mgm.json"))        # converts json to dict
    else:
        control_flow_mgm = create_control_flow_mgm_file()
    
    try:
        return control_flow_mgm
    except NameError:
      pass


# Create control flow file saving function

def save_control_flow_file(control_flow_mgm):
    control_flow_mgm_json = json.dumps(control_flow_mgm)            # convert dict to json
    os.chdir(path_dict['flow_control_path'])
    f = open("control_flow_mgm.json","w")
    f.write(control_flow_mgm_json)
    f.close()

## --- preprocessing --- ##

### --- MaStR --- ###

def single_xml_file_maStR_loader_for_data_inspection(file):
    # import xml
    filename_import = f'{file}.xml'
    raw_data_path_MaStR = path_dict['MaStR_xml']
    xml_path = os.path.realpath(f'{raw_data_path_MaStR}\\{filename_import}')
    tree = ET.parse(xml_path)
    xml_data = tree.getroot()
    xmlstr = ET.tostring(xml_data, encoding='utf-8', method='xml')
    data_dict_unity = dict(xmltodict.parse(xmlstr))

    try:
        return data_dict_unity
    except:
        pass

def extract_data_from_MaStR_from_xml_to_csv(prefix_data_type):
    ## start with single prefix_data_type input, enhance later with list
    # call the extraction worker in single or multiprocess mode
     
    # grap relevant files
    available_raw_data_MaStR = os.listdir(path_dict['MaStR_xml'])              # returns list with filenames
    substring_unity_type = prefix_data_type
    available_raw_data_MaStR_unity = []

    for file in available_raw_data_MaStR:       
        if substring_unity_type in file:
            available_raw_data_MaStR_unity.append(file.removesuffix('.xml'))
    
    number_files_of_this_unity_type = len(available_raw_data_MaStR_unity)

    # create multiprocess functions only if necessary
    if number_files_of_this_unity_type > 1:
        def extract_generel_storage_plants_data_to_multiple_csv_caller(available_raw_data_MaStR_unity, prefix_data_type):
            ''' make a list of files, start parallelization and call the worker in a pool

            If prefix_data_type is prefix_solar_unity or prefix_storage_unity, collect the availble xml-files and push them to the workers in multiprocessing.
            The worker is defined as extract_multiple_plants_data_to_multiple_csv_worker() in extract_solar_plants_data_to_multiple_csv_worker.py

            '''
            # make sure to start in work_path, so that if path_dict is generating os.getcwd() we get right rel pathes when calling 
            # extract_multiple_plants_data_to_multiple_csv_worker_2 and create new namespace
            os.chdir(path_dict['work_path'])

            # define max number of workers
            max_pool = min(len(available_raw_data_MaStR_unity), 14)
            # add later max available cores as further min arg
            
                    
            print(time.strftime("%H:%M:%S", time.localtime()) + ' : ' + str(len(available_raw_data_MaStR_unity)) + ' files of : ' + str(prefix_data_type) + ' : start Pool with number of workers : ' + str(max_pool))
            
            with Pool(max_pool) as p:
            #with Pool() as p:                                              # use default mode with all available cpus, but 24 workers are not faster as 12 workers
                results = p.map(extract_multiple_plants_data_to_multiple_csv_worker, available_raw_data_MaStR_unity)
            
            #print(results)

        def merge_generell_unity_csvs(prefix_data_type):
            # get single solar csvs
            available_raw_data_MaStR_csv = os.listdir(path_dict['single_MaStR_csv'])
            # clean up list
            archiv_folder_name = '_archiv'
            if archiv_folder_name in available_raw_data_MaStR_csv:
                available_raw_data_MaStR_csv.remove(archiv_folder_name)
            for csv in available_raw_data_MaStR_csv:
                if prefix_data_type not in csv:
                    available_raw_data_MaStR_csv.remove(csv)
            # sort csv in the original order from MaStR
            available_raw_data_MaStR_csv = sorted(available_raw_data_MaStR_csv, key=lambda x: int("".join([i for i in x if i.isdigit()])))

            # read first csv
            os.chdir(path_dict['single_MaStR_csv'])
            dataframe_unity = pd.read_csv(available_raw_data_MaStR_csv[0])
            
            # stick all the other csvs below
            for c in range(1,len(available_raw_data_MaStR_csv)):
                dataframe_unity = pd.concat([dataframe_unity, pd.read_csv(available_raw_data_MaStR_csv[c])], axis=0, ignore_index=False) 

            # delete the first row as its empty
            dataframe_unity = dataframe_unity.iloc[1: ,:]

            # through out empty column
            dataframe_unity = dataframe_unity.drop(dataframe_unity.columns[0], axis=1)

            # save the merged csv
            try:
                filename_export_part_1 = prefix_data_type
                filename_export_part_2 = file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix']
                filename_export = f'{filename_export_part_1}{filename_export_part_2}'
                os.chdir(path_dict['static_input'])
                dataframe_unity.to_csv(filename_export)
            except:
                pass
    
    # make announcements for long prefix_data_types
    estimated_duration_processing = {
        file_names_dict['filenames_raw']['prefix_solar_unity'] : 'this takes 7 h',
        file_names_dict['filenames_raw']['prefix_wind_unity'] : 'this takes 3 mins',
        file_names_dict['filenames_raw']['prefix_storage_unity'] : 'this takes 60 mins',
        file_names_dict['filenames_raw']['prefix_combustion_unity'] : 'this takes 14 mins',
        file_names_dict['filenames_raw']['prefix_biomass_unity'] : 'this takes 1 mins',
        file_names_dict['filenames_raw']['prefix_grid_Marktakteure_unity'] : 'this takes 45 mins',        
    }

    if prefix_data_type in estimated_duration_processing.keys():
        print(time.strftime("%H:%M:%S", time.localtime()) + ' : start processing : ' + str(prefix_data_type) + ' : ' + str(estimated_duration_processing[prefix_data_type]))
    elif prefix_data_type not in estimated_duration_processing.keys():
        print(time.strftime("%H:%M:%S", time.localtime()) + ' : start processing : ' + str(prefix_data_type))

    # serial processing for small batches
    if number_files_of_this_unity_type == 1:
        # worker do not need explicitly a return value
        extract_multiple_plants_data_to_multiple_csv_worker(available_raw_data_MaStR_unity[0])
    elif ((number_files_of_this_unity_type > 1) & (number_files_of_this_unity_type < 100)):
        extract_generel_storage_plants_data_to_multiple_csv_caller(available_raw_data_MaStR_unity, prefix_data_type)
        merge_generell_unity_csvs(prefix_data_type)

    else:
        pass
    
    ## normally that function doesn't have an output as saving results on hard drive, here for test purposes
    return_the_file_for_debugging = 1
    if return_the_file_for_debugging:
        if number_files_of_this_unity_type == 1:
            try:
                dataframe_unity = load_unity_extractions_from_MaStR(prefix_data_type)
                return dataframe_unity
            except:
                return 'no output possible'
        elif number_files_of_this_unity_type > 1:
            try:
                return 'this was a multiple xml file to csv export, look manually in _single_CSV'
            except:
                return 'no output possible'

def load_unity_extractions_from_MaStR(prefix_data_type):
    '''
    load csv files from 02_csv_static, df_operator_joined files should work also but not tested
    '''
    os.chdir(path_dict['static_input'])
    # load CSV
    filename_import_part_1 = prefix_data_type
    filename_import_part_2 = file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    # read data
    try:
        with open(str(filename_import)) as csv_file:
            unity_extractions_from_MaStR = pd.read_csv(csv_file, encoding='utf-8', sep=",")
    except UnicodeDecodeError:
        with open(str(filename_import)) as csv_file:
            #unity_extractions_from_MaStR = pd.read_csv(csv_file, encoding='utf-8', sep=",")
            #unity_extractions_from_MaStR = pd.read_csv(csv_file, encoding='cp273', sep=",")
            #unity_extractions_from_MaStR = pd.read_csv(csv_file, encoding='latin1', sep=",")
            unity_extractions_from_MaStR = pd.read_csv(csv_file, encoding='cp1252', sep=",")
    # delete first column
    unity_extractions_from_MaStR = unity_extractions_from_MaStR.drop(unity_extractions_from_MaStR.columns[[0]], axis=1)
    print(time.strftime("%H:%M:%S", time.localtime()) + ' : loading data : ' + str(filename_import))
    try:
        return unity_extractions_from_MaStR
    except:
        pass

def add_operator_information(prefix_data_type):
    # make announcements for long prefix_data_types
    estimated_duration_processing = {
        file_names_dict['filenames_raw']['prefix_solar_unity'] : 'this takes 7 h',
        file_names_dict['filenames_raw']['prefix_wind_unity'] : 'this takes 1 h',
        file_names_dict['filenames_raw']['prefix_storage_unity'] : 'this takes 60 mins',
        file_names_dict['filenames_raw']['prefix_combustion_unity'] : 'this takes 1,5 h',
        file_names_dict['filenames_raw']['prefix_biomass_unity'] : 'this takes 35 mins',
        file_names_dict['filenames_raw']['prefix_water_generator_unity'] : 'this takes 13 mins',        
    }
    if prefix_data_type in estimated_duration_processing.keys():
        print(time.strftime("%H:%M:%S", time.localtime()) + ' : start processing : ' + str(prefix_data_type) + ' : ' + str(estimated_duration_processing[prefix_data_type]))
    elif prefix_data_type not in estimated_duration_processing.keys():
        print(time.strftime("%H:%M:%S", time.localtime()) + ' : start processing : ' + str(prefix_data_type))
    # load data
    akteure_extractions_from_MaStR = load_unity_extractions_from_MaStR('Marktakteure')
    unity_extractions_from_MaStR = load_unity_extractions_from_MaStR(prefix_data_type)
    # define column names for easy call
    unit_column_name = file_names_dict['filenames_raw']['column_names_operator'][prefix_data_type]
    akteure_column_name_1 = file_names_dict['filenames_raw']['column_names_operator']['Marktakteure'][0]
    akteure_column_name_2 = file_names_dict['filenames_raw']['column_names_operator']['Marktakteure'][1]
    akteure_column_name_clear = file_names_dict['filenames_raw']['column_names_operator_clear_name_marktakteure']
    # collect operator information
    list_operator_ID_unit = list(unity_extractions_from_MaStR[unit_column_name])
    def get_clear_names_akteure(list_operator_ID_unit, akteure_extractions_from_MaStR):
        list_operator_ID_akteure_1 = []
        for i in range(len(list_operator_ID_unit)):
            if list_operator_ID_unit[i] != 'None':
                clearname = akteure_extractions_from_MaStR.loc[akteure_extractions_from_MaStR[akteure_column_name_1] == list_operator_ID_unit[i]][akteure_column_name_clear].values[0]
                list_operator_ID_akteure_1.append(clearname)
            elif list_operator_ID_unit[i] == 'None':
                list_operator_ID_akteure_1.append('None')
        return list_operator_ID_akteure_1

    list_operator_ID_akteure_1 = get_clear_names_akteure(list_operator_ID_unit, akteure_extractions_from_MaStR)
    unity_extractions_from_MaStR['operator'] = list_operator_ID_akteure_1
    # save in new csv
    try:
        filename_export_part_1 = prefix_data_type
        filename_export_part_2 = file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix_with_operator']
        filename_export = f'{filename_export_part_1}{filename_export_part_2}'
        os.chdir(path_dict['static_input'])
        unity_extractions_from_MaStR.to_csv(filename_export)
    except:
        print(time.strftime("%H:%M:%S", time.localtime()) + ' : error during saving result csv of : ' + str(prefix_data_type))


    try:
        return unity_extractions_from_MaStR
    except:
        return 'some error during return'

def check_complete_MaStR_xml_extractions():
    os.chdir(path_dict['static_input'])
    marker_operator = file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix_with_operator']
    marker_without_operator = file_names_dict['filenames_csv_exports_MaStR']['dataframe_unity_file_name_sufix']
    list_files = os.listdir(path_dict['static_input'])
    list_files_operator = []
    list_files_without_operator = []
    for filename in list_files:
        if marker_operator in filename:
            list_files_operator.append(filename)
        elif marker_without_operator in filename:
            list_files_without_operator.append(filename)
    list_files_operator = [filename.replace(marker_operator,'') for filename in list_files_operator]
    list_files_without_operator = [filename.replace(marker_without_operator,'') for filename in list_files_without_operator]
    list_files_without_operator = [filename for filename in list_files_without_operator if filename not in list_files_operator]
    return list_files_operator, list_files_without_operator

### --- SMARD.de --- ###

def preprocess_smard_historical_data():
    ''' preprocess downloads from smard.de and merge them to one df

    Download specs:
    Region: Land: Deutschland
    Auflösung: Viertelstunde (bei Installierter Leistung: Tag)
    Dateiformat: CSV    
    '''
    ## sort available time series data for processing    
    def sort_historical_smard_files():
        available_raw_data_Smard = os.listdir(path_dict['smard_historical_data'])              # returns list with filenames
        #print(available_raw_data_Smard)
        available_data_smard = {}
        available_data_smard['files'] = {}
        available_data_smard['beginning'] = {}
        available_data_smard['ending'] = {}
        available_data_smard['beginning_date'] = ''
        available_data_smard['ending_date'] = ''
        for feature in list(file_names_dict['filename_specs'].keys())[0:5]:
            available_data_smard['files'][feature] = []
            available_data_smard['beginning'][feature] = ''
            available_data_smard['ending'][feature] = ''
        for filesname in available_raw_data_Smard:
            for feature in list(file_names_dict['filename_specs'].keys())[0:5]:
                if file_names_dict['filename_specs'][feature] in filesname:
                    available_data_smard['files'][feature].append(filesname)
        for feature in list(file_names_dict['filename_specs'].keys())[0:5]:
            min_day = datetime.strptime('202304300000', '%Y%m%d%H%M')
            max_day = datetime.strptime('201501010000', '%Y%m%d%H%M')
            files_list = list(available_data_smard['files'][feature]).copy()
            for i in range(len(files_list)):
                files_list[i] = files_list[i].replace(file_names_dict['filename_specs'][feature],"")
                files_list[i] = files_list[i].replace(file_names_dict['filename_specs']['sufix_day'],"")
                files_list[i] = files_list[i].replace(file_names_dict['filename_specs']['sufix_15_min'],"")
                files_list[i] = files_list[i].replace(file_names_dict['filename_specs']['sufix_file'],"")
                files_list[i] = files_list[i].replace(file_names_dict['filename_specs']['delimiter'],"")
                start_file = datetime.strptime(str(files_list[i][0:11]), '%Y%m%d%H%M')
                end_file = datetime.strptime(str(files_list[i][12:23]), '%Y%m%d%H%M')
                if start_file < min_day:
                    min_day = start_file
                if end_file > max_day:
                    max_day = end_file
            available_data_smard['beginning'][feature] = min_day
            available_data_smard['ending'][feature] = max_day
        beginning = []
        ending = []
        for feature in list(file_names_dict['filename_specs'].keys())[0:5]:
            beginning.append(available_data_smard['beginning'][feature])
            ending.append(available_data_smard['ending'][feature])
        beginning_check = all(elem == beginning[0] for elem in beginning)
        ending_check = all(elem == ending[0] for elem in ending)
        if not(beginning_check and ending_check):
            available_data_smard['beginning_date'] = 'timeseries starting and ending datetime not homogenieus'
            available_data_smard['ending_date'] = 'timeseries starting and ending datetime not homogenieus'
        else:
            available_data_smard['beginning_date'] = beginning[0]
            available_data_smard['ending_date'] = ending[0]
        try:
            return available_data_smard
        except:
            pass
    
    ## extract time series per class
    def merge_all_historical_smard_files(available_data_smard):
        def load_and_clean_historical_smard_file_single(filename):
            # load file
            print(filename)
            os.chdir(path_dict['smard_historical_data'])
            debug_old = 1
            if debug_old:
                if str(file_names_dict['filename_specs']['prefix_installed_power']) in filename:
                    #print('use first if clause')
                    file = pd.read_csv(filename, sep=";", decimal=',')
                    file['time'] = pd.to_datetime(file['Datum'] + ' ' + file['Anfang'], format='%d.%m.%Y %H:%M')
                    file.set_index('time', inplace=True)
                    file = file.drop(['Datum', 'Anfang', 'Ende'], axis=1)
                    file = file.astype(str)
                    # get rid of decimal and thousand characters
                    for c in file.columns:
                        file.loc[:, c] = [ i.replace(".","") for i in file.loc[:, c] ]
                        file[c] = pd.to_numeric(file[c], errors='coerce')
                elif str(file_names_dict['filename_specs']['prefix_predicted_generation']) in filename:
                    #print('use first elif clause')
                    # with 'Anfang' as clock value no time jumps in time index, but delay of time index of 15 min
                    file = pd.read_csv(filename, sep=";", decimal=',', thousands='.', parse_dates={'time':['Datum', 'Anfang']}, dayfirst=True, dtype=file_names_dict['filename_specs']['dtypes_predicted_generation'], na_values=file_names_dict['filename_specs']['na_values_predicted_generation'])
                    file['time'] = pd.to_datetime(file['time'])
                    file.set_index('time', inplace=True)
                    file = file.drop(['Ende'], axis=1)
                    file.sort_values(by='time', inplace=True)
                elif str(file_names_dict['filename_specs']['prefix_measured_generation']) in filename:
                    #print('use first elif clause')
                    # with 'Anfang' as clock value no time jumps in time index, but delay of time index of 15 min
                    file = pd.read_csv(filename, sep=";", decimal=',', thousands='.', parse_dates={'time':['Datum', 'Anfang']}, dayfirst=True, dtype=file_names_dict['filename_specs']['dtypes_measured_generation'], na_values=file_names_dict['filename_specs']['na_values_measured_generation'])
                    file['time'] = pd.to_datetime(file['time'])
                    file.set_index('time', inplace=True)
                    file = file.drop(['Ende'], axis=1)
                    file.sort_values(by='time', inplace=True)
                elif str(file_names_dict['filename_specs']['prefix_predicted_consumption']) in filename:
                    #print('use first elif clause')
                    # with 'Anfang' as clock value no time jumps in time index, but delay of time index of 15 min
                    file = pd.read_csv(filename, sep=";", decimal=',', thousands='.', parse_dates={'time':['Datum', 'Anfang']}, dayfirst=True, dtype=file_names_dict['filename_specs']['dtypes_predicted_consumption'], na_values=file_names_dict['filename_specs']['na_values_predicted_consumption'])
                    file['time'] = pd.to_datetime(file['time'])
                    file.set_index('time', inplace=True)
                    file = file.drop(['Ende'], axis=1)
                    file.sort_values(by='time', inplace=True)
                elif str(file_names_dict['filename_specs']['prefix_measured_consumption']) in filename:
                    #print('use first elif clause')
                    # with 'Anfang' as clock value no time jumps in time index, but delay of time index of 15 min
                    file = pd.read_csv(filename, sep=";", decimal=',', thousands='.', parse_dates={'time':['Datum', 'Anfang']}, dayfirst=True, dtype=file_names_dict['filename_specs']['dtypes_measured_consumption'], na_values=file_names_dict['filename_specs']['na_values_measured_consumption'])
                    file['time'] = pd.to_datetime(file['time'])
                    file.set_index('time', inplace=True)
                    file = file.drop(['Ende'], axis=1)
                    file.sort_values(by='time', inplace=True)
                
                else:
                    file = pd.read_csv(filename, sep=";", decimal=',', thousands='.')
                    file['time'] = pd.to_datetime(file['Datum'] + ' ' + file['Anfang'], format='%d.%m.%Y %H:%M')
                    file.set_index('time', inplace=True)
                    file = file.drop(['Datum', 'Anfang', 'Ende'], axis=1)
            # clean column names
            substrings_to_delete = [' Berechnete Auflösungen', ' Originalauflösungen']
            for substring in substrings_to_delete:
                for c in file.columns:
                    if substring in c:
                        file.rename(columns={c: c.replace(substring, "")}, inplace=True)
            delete_space = " "
            for c in file.columns:
                if delete_space in c:
                    file.rename(columns={c: c.replace(delete_space, "_")}, inplace=True)
            # clean numeric columns
            manuell_numeric_cleaning = 0
            if manuell_numeric_cleaning:
                file = file.astype(str)
                # get rid of decimal and thousand characters
                for c in file.columns:
                    file.loc[:, c] = [ i.replace(".","") for i in file.loc[:, c] ]
                # drop missing values and set datatype to int
                special_cleaning_columns = ['Biomasse_[MWh]', 'Sonstige_Erneuerbare_[MWh]', 'Sonstige_Konventionelle_[MWh]']
                for c in file.columns:
                    if c in special_cleaning_columns:
                        file = file[file[c].str.contains("-") == False]
                #file = file.astype(int)
                file = file.convert_dtypes()
            automatic_numeric_cleaning = 0
            if automatic_numeric_cleaning:
                for c in file.columns:
                    #file.loc[:, c] = [ i.replace(",",".") for i in file.loc[:, c] ]
                    file[c] = file[c].astype(np.float64, errors='ignore')
                    file[c] = pd.to_numeric(file[c], errors='coerce')
            try:
                return file
            except:
                pass
        
        def save_historical_smard_file_single(feature, merge_file):
            filename_export_part_1 = file_names_dict['filename_specs'][feature]
            filename_export_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
            filename_export = f'{filename_export_part_1}{filename_export_part_2}'
            os.chdir(path_dict['preprocessed_time_series'])
            merge_file.to_csv(filename_export)

        for feature in list(file_names_dict['filename_specs'].keys())[0:5]:
            merge_file = load_and_clean_historical_smard_file_single(available_data_smard['files'][feature][0])
            for file in available_data_smard['files'][feature][1:]:
                #print(file)
                load_to_merge_file = load_and_clean_historical_smard_file_single(file)
                merge_file = pd.concat([merge_file, load_to_merge_file], axis=0, ignore_index=False)
            convert_amount_to_power = 1
            if convert_amount_to_power:
                substring_1 = '[MWh]'
                substring_2 = '[MW]'
                for c in merge_file.columns:
                    if substring_1 in str(c):
                        merge_file.rename(columns={c: c.replace(substring_1, substring_2)}, inplace=True)
                        merge_file[c.replace(substring_1, substring_2)] = merge_file[c.replace(substring_1, substring_2)]*4
                        print('changed column : ' + str(c) + ' : from MWh to MW')
            # save file
            save_historical_smard_file_single(feature, merge_file)

    #os.chdir(path_dict['smard_historical_data'])
    available_data_smard = sort_historical_smard_files()
    print(available_data_smard)
    merge_all_historical_smard_files(available_data_smard)

    try:
        pass
    except:
        pass


## ----- aggregation and plot functions for yupiter -----

def plot_historical_installed_power_timeseries(column_index):
    # load data
    feature = list(file_names_dict['filename_specs'].keys())[0]
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    time_serie_import = pd.read_csv(filename_import, index_col='time')
    def plot_historical_consumption(df,value_input):
        plot_title = f'timeseries installed power in [MW] : {value_input}'
        fig = px.line(df, x=df.index, y=value_input, title=plot_title)
        #fig.add_scatter(x=df.index, y=value_input_2 ,mode='lines')
        fig.show()
    # df.columns = {0: 'Biomasse [MW]', 1: 'Wasserkraft [MW]', 2: 'Wind Offshore [MW]', 3: Wind Onshore [MW], 4: Photovoltaik [MW], 
    #               5: 'Sonstige Erneuerbare [MW]', 6: 'Kernenergie [MW]', 7: 'Braunkohle [MW]',8: 'Steinkohle [MW]', 9: 'Erdgas [MW]', 
    #               10: 'Pumpspeicher [MW]', 11: 'Sonstige Konventionelle [MW]'}
    plot_historical_consumption(time_serie_import,time_serie_import.columns[column_index])
    try:
        return time_serie_import
    except:
        pass

def plot_historical_consumption_timeseries(column_index):
    # load data
    feature = list(file_names_dict['filename_specs'].keys())[3]
    print(path_dict['preprocessed_time_series'])
    print(os.getcwd())
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    time_serie_import = pd.read_csv(filename_import, index_col='time')
    def plot_historical_consumption(df,value_input, plot_title):
        fig = px.line(df, x=df.index, y=value_input, title=plot_title)
        #fig.add_scatter(x=df.index, y=value_input_2 ,mode='lines')
        fig.show()
        try:
            return fig
        except:
            pass
    # df.columns = {0: 'Gesamt_(Netzlast)_[MW]', 1: 'Residuallast_[MW]', 2: 'Pumpspeicher_[MW]'}
    fig = plot_historical_consumption(time_serie_import,time_serie_import.columns[column_index], 'historical consumption of Germany (15mins-values in [MW])')
    try:
        return time_serie_import, fig
    except:
        pass

def aggregate_historical_consumption():
    # load data
    feature = list(file_names_dict['filename_specs'].keys())[3]
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    #time_serie_import = pd.read_csv(filename_import, index_col='time')
    time_serie_import = pd.read_csv(filename_import)
    time_serie_import['time'] = pd.to_datetime(time_serie_import['time'])
    time_serie_import.rename(columns={'Gesamt_(Netzlast)_[MW]': 'Gesamt_(Netzlast)_[MWh]'}, inplace=True)
    time_serie_import.rename(columns={'Gesamt_(Netzlast)_[MW]': 'Gesamt_(Netzlast)_[MWh]'}, inplace=True)
    historical_consumption_per_year = time_serie_import.groupby(time_serie_import.time.dt.year)['Gesamt_(Netzlast)_[MWh]'].sum()/4
    historical_consumption_per_year_and_month = time_serie_import.groupby([time_serie_import.time.dt.year, time_serie_import.time.dt.month])['Gesamt_(Netzlast)_[MWh]'].sum()/4
    
    try:
        #return time_serie_import, historical_consumption_per_year
        return historical_consumption_per_year, historical_consumption_per_year_and_month
    except:
        pass

def plot_historical_generation_timeseries(column_index):
    # load data
    feature = list(file_names_dict['filename_specs'].keys())[1]
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    time_serie_import = pd.read_csv(filename_import, index_col='time')
    def plot_historical_generation(df,value_input):
        plot_title = f'historical generation of Germany (15mins-values in [MW]) : {value_input}'
        fig = px.line(df, x=df.index, y=value_input, title=plot_title)
        #fig.add_scatter(x=df.index, y=value_input_2 ,mode='lines')
        fig.show()
    # df.columns = {0: 'Biomasse [MW]', 1: 'Wasserkraft [MW]', 2: 'Wind Offshore [MW]', 3: Wind Onshore [MW], 4: Photovoltaik [MW], 
    #               5: 'Sonstige Erneuerbare [MW]', 6: 'Kernenergie [MW]', 7: 'Braunkohle [MW]',8: 'Steinkohle [MW]', 9: 'Erdgas [MW]', 
    #               10: 'Pumpspeicher [MW]', 11: 'Sonstige Konventionelle [MW]'}
    plot_historical_generation(time_serie_import,time_serie_import.columns[column_index])
    try:
        return time_serie_import
    except:
        pass

def aggregate_historical_generation():
    # load data
    feature = list(file_names_dict['filename_specs'].keys())[1]
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    dtypes_preprocessed_measured_generation_power = file_names_dict['filename_specs']['dtypes_measured_generation_power']
    time_serie_import = pd.read_csv(filename_import, parse_dates=['time'], dtype=dtypes_preprocessed_measured_generation_power, na_values=file_names_dict['filename_specs']['na_values_measured_generation_power'])
    time_serie_import['time'] = pd.to_datetime(time_serie_import.time, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    time_serie_import.set_index('time', inplace=True)

    # build target df for aggregated data, extract data and clean df, sum(MW)/4 = sum(MWh)
    historical_generation_per_year = pd.DataFrame(columns=time_serie_import.columns, index=range(min(list(time_serie_import.index)).year, max(list(time_serie_import.index)).year+1))
    for c in time_serie_import.columns:
        historical_generation_per_year[c] = time_serie_import.groupby(time_serie_import.index.year)[c].sum()/4

    historical_generation_per_year_and_month = {}
    for c in time_serie_import.columns:
        historical_generation_per_year_and_month[c] = time_serie_import[c].groupby([time_serie_import.index.year, time_serie_import.index.month]).sum()/4
    historical_generation_per_year_and_month = pd.DataFrame.from_dict(historical_generation_per_year_and_month)

    # clean column names
    data_frames = [historical_generation_per_year, historical_generation_per_year_and_month]
    substrings_to_replace = ['[MW]']
    for file in data_frames:   
        for substring in substrings_to_replace:
            for c in file.columns:
                if substring in c:
                    file.rename(columns={c: c.replace(substring, "[MWh]")}, inplace=True)
    
    try:
        #return time_serie_import
        return historical_generation_per_year, historical_generation_per_year_and_month
    except:
        pass


## ----- aggregation and plot functions for python -----

### --- SMARD.de data --- ###
#### --- timeseries 15 min --- ####
def load_historical_consumption_as_df():
    feature = list(file_names_dict['filename_specs'].keys())[3]
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    time_serie_import = pd.read_csv(filename_import, index_col='time')
    try:
        return time_serie_import
    except:
        pass

def load_historical_generation_as_df():
    feature = list(file_names_dict['filename_specs'].keys())[1]
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    time_serie_import = pd.read_csv(filename_import, index_col='time')
    try:
        return time_serie_import
    except:
        pass

def load_historical_gen_and_con_in_one_df():
    con_df = load_historical_consumption_as_df()
    gen_df = load_historical_generation_as_df()
    df = pd.concat([con_df, gen_df], axis=1)
    try:
        return df
    except:
        pass

def plot_single_con_gen(df, feature):
        ## df.columns = {0: 'Biomasse [MW]', 1: 'Wasserkraft [MW]', 2: 'Wind Offshore [MW]', 3: Wind Onshore [MW], 4: Photovoltaik [MW], 
        ##               5: 'Sonstige Erneuerbare [MW]', 6: 'Kernenergie [MW]', 7: 'Braunkohle [MW]',8: 'Steinkohle [MW]', 9: 'Erdgas [MW]', 
        ##               10: 'Pumpspeicher [MW]', 11: 'Sonstige Konventionelle [MW]'}
    plot_title = f'timeseries consumed and generated amounts in [MWh] : {feature}'
    fig = px.line(df, x=df.index, y=df[feature], title=plot_title)
    try:
        return fig
    except:
        pass

def plot_all_vectors_con_gen_amounts(df):
        ## df.columns = {0: 'Biomasse [MW]', 1: 'Wasserkraft [MW]', 2: 'Wind Offshore [MW]', 3: Wind Onshore [MW], 4: Photovoltaik [MW], 
        ##               5: 'Sonstige Erneuerbare [MW]', 6: 'Kernenergie [MW]', 7: 'Braunkohle [MW]',8: 'Steinkohle [MW]', 9: 'Erdgas [MW]', 
        ##               10: 'Pumpspeicher [MW]', 11: 'Sonstige Konventionelle [MW]'}
    years = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S') for i in list(df.index)]
    years = [i.year for i in years]
    years = sorted(list(set(years)))

    dff = df.reset_index()
    dff['year'] = [datetime.strptime(i, '%Y-%m-%d %H:%M:%S') for i in list(dff['time'])]
    dff['year'] = [i.year for i in list(dff['year'])]
    df_aggregate = dff.groupby(['year']).sum()
    df_aggregate = df_aggregate/4
    reversed_ordered_features = list(df_aggregate.columns)
    drop_con_features = 1
    if drop_con_features:
        list_drop_con_features = ['Gesamt_(Netzlast)']
        #for feature in 
    colors = dict.fromkeys(reversed_ordered_features)
    for key in list(colors.keys()):
        for i in list(model_dict['color_of_energy'].keys()):
            if i in key:
                generic_key = i
                colors[key] = model_dict['color_of_energy'][generic_key]
    print(colors)
    fig = go.Figure()
    try:
        for feature in reversed_ordered_features:
            fig.add_traces(go.Bar(x=df_aggregate.index, y=df_aggregate[feature], name=feature,  marker_color=colors[feature]))

        # Change the bar mode
        fig.update_layout(barmode='stack', title={'text': 'Installed generation power per category in [MW]'})
        #fig.update_xaxes(title_text='Installed Power [MW]')
    except:
        print('plot problem')
    try:
        return fig, df_aggregate
    except:
        try:
            return df_aggregate
        except:
            pass

def plot_single_raw_time_serie(time_serie, column_index):
    value_input = time_serie.columns[column_index]
    plot_title = f'{value_input.replace("_[MW]", "")} : raw timeserie in [MW] : '
    fig = px.line(time_serie, x=time_serie.index, y=value_input, title=plot_title)
    try:
        return fig
    except:
        pass

def aggregate_energy_amounts_as_df(time_serie, feature):
    time_serie = time_serie.reset_index(names='time')
    time_serie['time'] = pd.to_datetime(time_serie['time'])

    energy_amount_per_year = time_serie.groupby(time_serie.time.dt.year)[feature].sum()/4
    energy_amount_per_year_per_year_and_month = time_serie.groupby([time_serie.time.dt.year, time_serie.time.dt.month])[feature].sum()/4
    energy_amount_per_year_per_day = time_serie.groupby(time_serie.time.dt.date)[feature].sum()/4
    
    def aggregate_timeseries_per_week(time_serie_import):
        def get_previous_monday_from_datetime(datetime):
            return pd.to_datetime((datetime - timedelta(days=(datetime.isoweekday()-1), hours=datetime.hour, minutes=datetime.minute)))
        week_index_counter = 0
        date_last_monday = get_previous_monday_from_datetime(time_serie_import['time'][0])
        summed_power_of_current_week = 0
        energy_amount_per_week = pd.DataFrame(columns=['week_index', feature, 'date_last_monday'])
        for i in list(range(time_serie_import.shape[0])):
            if get_previous_monday_from_datetime(time_serie_import['time'][i]) == date_last_monday:
                summed_power_of_current_week += time_serie_import[feature][i]
            elif (get_previous_monday_from_datetime(time_serie_import['time'][i]) != date_last_monday):
                df = pd.DataFrame(data={'week_index' : [week_index_counter], 
                                        feature : [summed_power_of_current_week], 
                                        'date_last_monday' : [date_last_monday]})
                energy_amount_per_week = pd.concat([energy_amount_per_week, df])
                week_index_counter += 1
                date_last_monday = get_previous_monday_from_datetime(time_serie_import['time'][i])
                summed_power_of_current_week = 0
        energy_amount_per_week[feature] = energy_amount_per_week[feature]/(4)
        energy_amount_per_week.rename(columns={feature : f'{feature.replace("_[MW]", "_[MWh/w]")}'}, inplace=True)
        energy_amount_per_week.set_index('date_last_monday', inplace=True)
        energy_amount_per_week.drop(columns='week_index', inplace=True)
        try:
            return energy_amount_per_week
        except:
            pass
    energy_amount_per_week = aggregate_timeseries_per_week(time_serie)
    
    try:
        return energy_amount_per_year, energy_amount_per_year_per_year_and_month, energy_amount_per_year_per_day, energy_amount_per_week
    except:
        pass

def plot_aggregates_energy_amounts_per_year(time_serie_import):
    time_serie_import = time_serie_import/1000000
    title_str = f'{time_serie_import.name.replace("_[MWh]", "")} : per year in [TWh/a]'
    fig = px.bar(time_serie_import, x=time_serie_import.index, y=time_serie_import.values, title=title_str,
                 labels = {
                    'time' : 'year',
                    'y' : '[TWh/a]'
                 })
    try:
        return fig
    except:
        pass

def plot_aggregates_energy_amounts_per_year_and_month(time_serie_import):
    time_serie_import = time_serie_import/1000000
    time_serie_import.index = time_serie_import.index.rename(['year', 'month'])
    time_serie_import = time_serie_import.reset_index()
    time_serie_import['month_index'] = time_serie_import['year'].astype(str) + '-' + time_serie_import['month'].astype(str)
    time_serie_import.index = pd.to_datetime(time_serie_import['month_index'])
    time_serie_import.resample('M').last()
    time_serie_import.drop(columns=['year', 'month', 'month_index'], inplace=True)
    title_str = f'{time_serie_import.columns[0].replace("_[MWh]", "")} : per year and month in [TWh/m]'
    #title_str = 'electricity consumption of Germany per year and month in [TWh/m]'
    fig = px.bar(time_serie_import, x=time_serie_import.index, y=time_serie_import[time_serie_import.columns[0]], title=title_str,
                 labels = {
                    'month_index' : 'month',
                    time_serie_import.columns[0] : '[TWh/m]'
                 })
    try:
        return fig
    except:
        pass

def plot_aggregates_energy_amounts_per_day(time_serie_import):
    time_serie_import = time_serie_import/1000000
    title_str = f'{time_serie_import.name.replace("_[MWh]", "")} : per day in [TWh/d]'
    fig = px.bar(time_serie_import, x=time_serie_import.index, y=time_serie_import.values, title=title_str,
                 labels = {
                    'y' : '[TWh/d]',
                    'time' : 'time'
                 })
    try:
        return fig
    except:
        pass

def plot_aggregates_energy_amounts_per_week(time_serie_import):
    time_serie_import[time_serie_import.columns[0]] = time_serie_import[time_serie_import.columns[0]]/(1000000)
    #time_serie_import.rename(columns={time_serie_import.columns[0] : 'energy_amount_per_week [TWh/m]'}, inplace=True)
    title_str = f'{time_serie_import.columns[0].replace("_[MWh/w]", "")} : per week in [TWh/w]'
    #title_str = 'electricity consumption of Germany per week in [TWh/w]'
    fig = px.bar(time_serie_import, x=time_serie_import.index, y=time_serie_import.columns[0], title=title_str,
                    labels = {
                    time_serie_import.columns[0] : '[TWh/w]',
                    'date_last_monday' : 'time'
                    })
    try:
        return fig
    except:
        pass

def create_df_for_yearly_heatmap_hour_weekday(time_serie_import, feature, input_year=''):
    time_serie_import = time_serie_import.reset_index()
    time_serie_import['time'] = pd.to_datetime(time_serie_import['time'])
    list_year = [i.isocalendar()[0] for i in list(time_serie_import['time'])]
    list_weekday = [i.isoweekday() for i in list(time_serie_import['time'])]
    list_hour = [i.hour for i in list(time_serie_import['time'])]
    time_serie_import['year'] = list_year
    time_serie_import['weekday'] = list_weekday
    time_serie_import['hour'] = list_hour
    if input_year == '':
        heatmap_df_one_year = time_serie_import
    elif type(input_year) == int:
        heatmap_df_one_year = time_serie_import[time_serie_import['year'] == input_year]
    heatmap_df_h_wd = heatmap_df_one_year.groupby([heatmap_df_one_year['hour'], heatmap_df_one_year['weekday']])[feature].mean()
    heatmap_df_h_wd = heatmap_df_h_wd.reset_index()
    try:
        return heatmap_df_h_wd, input_year
    except:
        pass

def plot_timeseries_heatmap_hour_weekday(heatmap_df, feature, input_year=''):
    # change to GW
    heatmap_df[feature] = heatmap_df[feature]/1000
    heatmap_df.rename(columns={feature: feature.replace('_[MW]', '')}, inplace=True)
    z = []
    for d in list(range(1,8)):
          z.append(list(heatmap_df[feature.replace('_[MW]', '')].loc[heatmap_df['weekday'] == d]))
    z_list = z
    y_list = ['Monday', 'Thuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    x_list = list(range(0,24))
    if input_year == '':
        title_str = f'{list(heatmap_df.columns)[2]} : all available years : mean per hours and weekdays in [GW]'
    elif type(input_year) == int:
        title_str = f'{list(heatmap_df.columns)[2]} : {input_year} : mean per hours and weekdays in [GW]'
    fig = go.Figure(data=go.Heatmap(
                    z=z_list[::-1],
                    x=x_list,
                    y=y_list[::-1],
                    hoverongaps = False
                    ))
    fig.update_layout(
        title = title_str,
        xaxis_title = 'hour',
    )
    try:
        return fig
    except:
        pass

def create_df_for_yearly_heatmap_hour_month(time_serie_import, feature, input_year=''):
    time_serie_import = time_serie_import.reset_index()
    time_serie_import['time'] = pd.to_datetime(time_serie_import['time'])
    list_year = [i.isocalendar()[0] for i in list(time_serie_import['time'])]
    list_month = [i.month for i in list(time_serie_import['time'])]
    list_hour = [i.hour for i in list(time_serie_import['time'])]
    time_serie_import['year'] = list_year
    time_serie_import['month'] = list_month
    time_serie_import['hour'] = list_hour
    if input_year == '':
        heatmap_df_one_year = time_serie_import
    elif type(input_year) == int:
        heatmap_df_one_year = time_serie_import[time_serie_import['year'] == input_year]
    heatmap_df_h_m = heatmap_df_one_year.groupby([heatmap_df_one_year['hour'], heatmap_df_one_year['month']])[feature].mean()
    heatmap_df_h_m = heatmap_df_h_m.reset_index()
    try:
        return heatmap_df_h_m, input_year
    except:
        pass

def plot_timeseries_heatmap_hours_month(heatmap_df, feature, input_year=''):
    # change to GW
    heatmap_df[feature] = heatmap_df[feature]/1000
    heatmap_df.rename(columns={feature: feature.replace('_[MW]', '')}, inplace=True)
    z = []
    for m in list(range(1,13)):
          z.append(list(heatmap_df[feature.replace('_[MW]', '')].loc[heatmap_df['month'] == m]))
    z_list = z
    y_list = ['January', 'February', 'March', 'Avril', 'Mai', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    x_list = list(range(0,24))
    if input_year == '':
        title_str = f'{list(heatmap_df.columns)[2]} : all available years : mean per hours and month in [GW]'
    elif type(input_year) == int:
        title_str = f'{list(heatmap_df.columns)[2]} : {input_year} : mean per hours and month in [GW]'  
    fig = go.Figure(data=go.Heatmap(
                    z=z_list[::-1],
                    x=x_list,
                    y=y_list[::-1],
                    hoverongaps = False
                    ))
    fig.update_layout(
        title = title_str,
        xaxis_title = 'hour',
    )
    try:
        return fig
    except:
        pass

def plot_annual_load_duration_curve_multiple_years(time_serie_import, feature):
    time_serie_import = time_serie_import.reset_index()
    time_serie_import['time'] = pd.to_datetime(time_serie_import['time'])
    list_year = [i.isocalendar()[0] for i in list(time_serie_import['time'])]
    time_serie_import['year'] = list_year
    list_dict = {}
    for y in list(time_serie_import['year'].unique()):
        df_year = time_serie_import[time_serie_import['year'] == y]
        df_ldc = df_year.sort_values(by=[feature], ascending=False)[feature]
        df_ldc.index = list(range(1, len(df_ldc)+1))
        list_dict[y] = df_ldc
        pass
    df_ldc_multiple = pd.DataFrame(list_dict)
    df_ldc_multiple = df_ldc_multiple/1000
    plot_title = f'{feature.replace("_[MW]", "")} : {list_year[0]} till {list_year[-1]} : annual load duration curve in [GW]'
    use_plotly = 1
    if use_plotly:
        fig = px.line(df_ldc_multiple, x=df_ldc_multiple.index/4, y=df_ldc_multiple.columns, title=plot_title)
        fig.update_layout(
            xaxis_title = 'hour',
            yaxis_title = 'power [GW]'
        )
    if not use_plotly:
        sns.set_theme()
        fig = sns.lineplot(data=df_ldc_multiple)
    try:
        return fig, df_ldc_multiple
    except:
        pass

def analyse_leap_years_and_summertime(time_serie_import):
    time_serie_import = time_serie_import.reset_index()
    time_serie_import['time'] = pd.to_datetime(time_serie_import['time'])
    time_serie_export = pd.DataFrame(columns=time_serie_import.columns)
    for y in list(model_dict['leap_years'].keys()):
        time_serie_extract = pd.DataFrame(columns=time_serie_import.columns)
        if model_dict['leap_years'][y]['switch_year']:
            timedelta_inspection_hours = 25
            switch_year_date = datetime(int(y), 2, 29)
            start_extract = switch_year_date - timedelta(hours=timedelta_inspection_hours)
            end_extract = switch_year_date + timedelta(hours=timedelta_inspection_hours)
            mask = (time_serie_import['time'] > start_extract) & (time_serie_import['time'] <= end_extract)
            #df = time_serie_import.loc[(time_serie_import['time'] > start_extract) & (time_serie_import['time'] < end_extract)]
            df = time_serie_import.loc[mask]
            time_serie_extract = pd.concat([time_serie_extract, df])
        timedelta_inspection_hours = 2
        for extract_date in [model_dict['leap_years'][y]['spring'], model_dict['leap_years'][y]['autum']]:
            start_extract = extract_date - timedelta(hours=timedelta_inspection_hours)
            end_extract = extract_date + timedelta(hours=timedelta_inspection_hours)
            mask = (time_serie_import['time'] > start_extract) & (time_serie_import['time'] <= end_extract)
            df = time_serie_import.loc[mask]
            time_serie_extract = pd.concat([time_serie_extract, df])    
        
        time_serie_export = pd.concat([time_serie_export, time_serie_extract])
    try:
        return time_serie_export
    except:
        pass

def plot_annual_load_duration_curve_multiple_years_2(time_serie_import, feature):
    time_serie_import = time_serie_import.reset_index()
    time_serie_import['time'] = pd.to_datetime(time_serie_import['time'])
    list_year = [i.isocalendar()[0] for i in list(time_serie_import['time'])]
    time_serie_import['year'] = list_year
    time_serie_import['index_year'] = ''
    time_serie_export = pd.DataFrame(columns=time_serie_import.columns)
    for y in list(time_serie_import['year'].unique()):
        df_year = time_serie_import[time_serie_import['year'] == y]
        df_ldc = df_year.sort_values(by=[feature], ascending=False)[feature]
        df_ldc['index_year'] = list(range(1, len(df_ldc)+1))
        time_serie_export = pd.concat([time_serie_export, df_ldc])
        pass
    time_serie_export[feature] = time_serie_export[feature]/1000
    plot_title = f'{feature.replace("_[MW]", "")} : {list_year[0]} till {list_year[-1]} : annual load duration curve in [GW]'
    use_plotly = 1
    if use_plotly:
        fig = px.line(time_serie_export, x=time_serie_export['index_year']/4, y=time_serie_export[feature], title=plot_title, color=time_serie_export['year'])
        fig.update_layout(
            xaxis_title = 'hour',
            yaxis_title = 'power [GW]'
        )
    if not use_plotly:
        sns.set_theme()
        fig = sns.lineplot(data=df_ldc_multiple)
    try:
        return fig, time_serie_export
    except:
        pass


#### --- installed power --- ####
def load_installed_power_as_df():
    # load data
    feature = list(file_names_dict['filename_specs'].keys())[0]
    os.chdir(path_dict['preprocessed_time_series'])
    filename_import_part_1 = file_names_dict['filename_specs'][feature]
    filename_import_part_2 = file_names_dict['filenames_csv_exports_historical_Smard']['sufix_single_merged_csv']
    filename_import = f'{filename_import_part_1}{filename_import_part_2}'
    time_serie_import = pd.read_csv(filename_import, index_col='time')
    try:
        return time_serie_import
    except:
        pass

def plot_single_installed_power(df, feature):
        ## df.columns = {0: 'Biomasse [MW]', 1: 'Wasserkraft [MW]', 2: 'Wind Offshore [MW]', 3: Wind Onshore [MW], 4: Photovoltaik [MW], 
        ##               5: 'Sonstige Erneuerbare [MW]', 6: 'Kernenergie [MW]', 7: 'Braunkohle [MW]',8: 'Steinkohle [MW]', 9: 'Erdgas [MW]', 
        ##               10: 'Pumpspeicher [MW]', 11: 'Sonstige Konventionelle [MW]'}
    plot_title = f'timeseries installed power in [MW] : {feature}'
    fig = px.line(df, x=df.index, y=df[feature], title=plot_title)
    try:
        return fig
    except:
        pass

def plot_all_vectors_installed_power(df):
        ## df.columns = {0: 'Biomasse [MW]', 1: 'Wasserkraft [MW]', 2: 'Wind Offshore [MW]', 3: Wind Onshore [MW], 4: Photovoltaik [MW], 
        ##               5: 'Sonstige Erneuerbare [MW]', 6: 'Kernenergie [MW]', 7: 'Braunkohle [MW]',8: 'Steinkohle [MW]', 9: 'Erdgas [MW]', 
        ##               10: 'Pumpspeicher [MW]', 11: 'Sonstige Konventionelle [MW]'}
    years = [datetime.strptime(i, '%Y-%m-%d') for i in list(df.index)]
    years = [i.year for i in years]
    years = sorted(list(set(years)))

    dff = df.reset_index()
    dff['year'] = [datetime.strptime(i, '%Y-%m-%d') for i in list(dff['time'])]
    dff['year'] = [i.year for i in list(dff['year'])]
    df_aggregate = dff.groupby(['year']).mean()
    reversed_ordered_features = list(df_aggregate.columns)
    colors = dict.fromkeys(reversed_ordered_features)
    for key in list(colors.keys()):
        for i in list(model_dict['color_of_energy'].keys()):
            if i in key:
                generic_key = i
                colors[key] = model_dict['color_of_energy'][generic_key]
    fig = go.Figure()
    for feature in reversed_ordered_features:
        fig.add_traces(go.Bar(x=df_aggregate.index, y=df_aggregate[feature], name=feature,  marker_color=colors[feature]))

    # Change the bar mode
    fig.update_layout(barmode='stack', title={'text': 'Installed generation power per category'})
    fig.update_yaxes(title_text='Installed Power [MW]')
    try:
        return fig, df_aggregate
    except:
        pass

### --- geodata --- ###

def load_geojson():
    os.chdir(path_dict['postcodes_geodata'])
    filename = 'postleitzahlen.geojson'
    print(time.strftime("%H:%M:%S", time.localtime()) + ' : start loading geojson : this takes 30 sec')
    gdf = geopandas.read_file(filename)
    gdff = gdf.copy()
    gdff['boundary'] = gdff.boundary
    gdff['centroid'] = gdff.centroid
    gdff['area'] = gdff.area
    # concvert map-file
    gdf_transformed = gdf.copy()
    gdf_transformed.to_file('main_map.geojson', driver='GeoJSON')
    try:
        return gdf, gdff, gdf_transformed
    except:
        pass


### --- MaStR data --- ###

def aggregate_MaStR_extraction_per_postcode(df_MaStR, prefix_data_type, aggregation_feature):
    # aggregation fct
    def aggregate_per_postcode_first_step(df_MaStR, aggregation_feature):
        df_MaStR_postcode_aggregates = pd.DataFrame(df_MaStR.groupby('Postleitzahl')[aggregation_feature].sum())
        df_MaStR_postcode_aggregates = df_MaStR_postcode_aggregates.reset_index()
        return df_MaStR_postcode_aggregates
    
    def aggregate_per_postcode_second_step(df_MaStR_postcode_aggregates):
        df_MaStR_postcode_aggregates['Postleitzahl'] = [int(float(i)) for i in list(df_MaStR_postcode_aggregates['Postleitzahl'])]
        df_MaStR_postcode_aggregates['Postleitzahl'] = df_MaStR_postcode_aggregates['Postleitzahl'].map(str)
        df_MaStR_postcode_aggregates['Postleitzahl'] = df_MaStR_postcode_aggregates['Postleitzahl'].str.zfill(5)
        return df_MaStR_postcode_aggregates
    
    def aggregate_per_postcode(df_MaStR, aggregation_feature):
        df_MaStR_postcode_aggregates = aggregate_per_postcode_first_step(df_MaStR, aggregation_feature)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_second_step(df_MaStR_postcode_aggregates)
        return df_MaStR_postcode_aggregates


    def aggregate_per_postcode_cleaning_spelling(df_MaStR):
        # clean spelling errors
        try:
            df_MaStR['Postleitzahl'][df_MaStR['Postleitzahl'] == '01-793'] = '01793'
        except:
            pass
        try:    
            df_MaStR['Postleitzahl'][df_MaStR['Postleitzahl'] == '190 16'] = '19016'
        except:
            pass
        try:
            df_MaStR['Postleitzahl'][df_MaStR['Postleitzahl'] == '40-114'] = '40114'            
        except:
            pass
        # return cleaned df
        try:
            return df_MaStR
        except:
            pass

    def aggregate_per_postcode_drop_non_german_postcodes(df_MaStR_postcode_aggregates):
        other_postcodes = ['A-6773', 'A-6774', 'A-6793', 'A-6794', '1082 MK', '2235 CJ', '3068 AV', '3707GL', '3068AV', '6211CJ', '7447 HE', '9821PN', 'A-1030', 'BT9 6EU', 'D02HY05', 'DK-1364', 'DK-4000', 'DK-8250', 'DK-9500', 'E14 5NJ', 'EC4A 4AB', 'L-1460', 'NL-7587', 'NY 10282', 'SE1 5JN', 'SE1 7NA', 'SW1E 5JL', 'SW1E 6AJ', 'T2P 1E3', 'WC1N 3AX', 'WC2N 6HT']
        # A-6773 is Rodundwerk in Lichtenstein, skip for analysis
        for i in range(df_MaStR_postcode_aggregates.shape[0]):
            if df_MaStR_postcode_aggregates['Postleitzahl'][i] in other_postcodes:
                df_MaStR_postcode_aggregates = df_MaStR_postcode_aggregates.drop(labels=[i], axis=0)
        df_MaStR_postcode_aggregates['Postleitzahl'] = [int(float(i)) for i in list(df_MaStR_postcode_aggregates['Postleitzahl'])]
        df_MaStR_postcode_aggregates['Postleitzahl'] = df_MaStR_postcode_aggregates['Postleitzahl'].map(str)
        df_MaStR_postcode_aggregates['Postleitzahl'] = df_MaStR_postcode_aggregates['Postleitzahl'].str.zfill(5)
        # return cleaned df
        try:
            return df_MaStR_postcode_aggregates
        except:
            pass


    # some individual preprocessing
    if prefix_data_type == str(file_names_dict['filenames_raw']['prefix_solar_unity']):
        # pre-data-cleaning for solar dataset
        df_MaStR = aggregate_per_postcode_cleaning_spelling(df_MaStR)          
        df_MaStR_postcode_aggregates = aggregate_per_postcode_first_step(df_MaStR, aggregation_feature)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_drop_non_german_postcodes(df_MaStR_postcode_aggregates)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_second_step(df_MaStR_postcode_aggregates)
    elif prefix_data_type == str(file_names_dict['filenames_raw']['prefix_water_generator_unity']):
        df_MaStR_postcode_aggregates = aggregate_per_postcode_first_step(df_MaStR, aggregation_feature)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_drop_non_german_postcodes(df_MaStR_postcode_aggregates)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_second_step(df_MaStR_postcode_aggregates)

    elif prefix_data_type == str(file_names_dict['filenames_raw']['prefix_storage_unity']):
        df_MaStR = aggregate_per_postcode_cleaning_spelling(df_MaStR)          
        df_MaStR_postcode_aggregates = aggregate_per_postcode_first_step(df_MaStR, aggregation_feature)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_drop_non_german_postcodes(df_MaStR_postcode_aggregates)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_second_step(df_MaStR_postcode_aggregates)

    elif prefix_data_type == str(file_names_dict['filenames_raw']['prefix_power_consumer_unity']):
        df_MaStR_postcode_aggregates = aggregate_per_postcode_first_step(df_MaStR, aggregation_feature)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_drop_non_german_postcodes(df_MaStR_postcode_aggregates)
        df_MaStR_postcode_aggregates = aggregate_per_postcode_second_step(df_MaStR_postcode_aggregates)
    
    else:
        df_MaStR_postcode_aggregates = aggregate_per_postcode(df_MaStR, aggregation_feature)

    try:
        return df_MaStR_postcode_aggregates
    except:
        pass


## ----- earlier versions of plotly - python fct -----
def plot_historical_consumption(df):
        plot_title = 'historical consumption of Germany (15mins-values in [MW])'
        ##column_index = {0: 'Gesamt_(Netzlast)_[MW]', 1: 'Residuallast_[MW]', 2: 'Pumpspeicher_[MW]'}
        column_index = 0
        value_input = df.columns[column_index]
        fig = px.line(df, x=df.index, y=value_input, title=plot_title)
        #fig.add_scatter(x=df.index, y=value_input_2 ,mode='lines')
        #fig.show()
        try:
            return fig
        except:
            pass

def plot_historical_generation_single(df, column_index):
        ## df.columns = {0: 'Biomasse [MW]', 1: 'Wasserkraft [MW]', 2: 'Wind Offshore [MW]', 3: Wind Onshore [MW], 4: Photovoltaik [MW], 
        ##               5: 'Sonstige Erneuerbare [MW]', 6: 'Kernenergie [MW]', 7: 'Braunkohle [MW]',8: 'Steinkohle [MW]', 9: 'Erdgas [MW]', 
        ##               10: 'Pumpspeicher [MW]', 11: 'Sonstige Konventionelle [MW]'}
    value_input = df.columns[column_index]
    plot_title = f'historical generation of Germany (15mins-values in [MW]) : {value_input}'
    fig = px.line(df, x=df.index, y=value_input, title=plot_title)
    try:
        return fig
    except:
        pass

def plot_annual_load_duration_curve(time_serie_import, input_year, feature):
    time_serie_import = time_serie_import.reset_index()
    time_serie_import['time'] = pd.to_datetime(time_serie_import['time'])
    list_year = [i.isocalendar()[0] for i in list(time_serie_import['time'])]
    time_serie_import['year'] = list_year
    df_year = time_serie_import[time_serie_import['year'] == input_year]
    df_ldc = df_year.sort_values(by=[feature], ascending=False)[feature]
    #df_ldc = df_ldc.reset_index()
    df_ldc.index = list(range(1, len(df_ldc)+1))
    plot_title = f'{feature.replace("_[MW]", "")} : {input_year} : annual load duration curve in [MW]'
    fig = px.line(df_ldc, x=df_ldc.index/4, y=feature, title=plot_title)
    fig.update_layout(
        xaxis_title = 'hour',
    )
    try:
        return fig
    except:
        pass

## ----- previous functions bootcamp -----

def aggregate_static_data_MaStR():
    # read solar data
    os.chdir(path_dict['static_input'])
    solar_df = pd.read_csv(file_names_dict['filenames_csv_exports_MaStR']['dataframe_solar_file_name_prefix'])
    solar_df = solar_df.drop(solar_df.columns[0], axis=1)

    # get aggregated data from solar dataset
    descriptives_dataframe_solar = {}

    descriptives_dataframe_solar['count_Bruttoleistung'] = float(round(solar_df['Bruttoleistung'].loc[solar_df['operating'] == 1].count(), 0))
    descriptives_dataframe_solar['sum_Bruttoleistung'] = float(round(solar_df['Bruttoleistung'].loc[solar_df['operating'] == 1].sum(), 0))
    descriptives_dataframe_solar['mean_Bruttoleistung'] = float(round(solar_df['Bruttoleistung'].loc[solar_df['operating'] == 1].mean(), 1))

    descriptives_dataframe_solar['count_Nettoleistung'] = float(round(solar_df['Nettonennleistung'].loc[solar_df['operating'] == 1].count(), 0))
    descriptives_dataframe_solar['sum_Nettoleistung'] = float(round(solar_df['Nettonennleistung'].loc[solar_df['operating'] == 1].sum(), 0))
    descriptives_dataframe_solar['mean_Nettoleistung'] = float(round(solar_df['Nettonennleistung'].loc[solar_df['operating'] == 1].mean(), 1))

    descriptives_dataframe_solar['count_P_FU'] = float(round(solar_df['ZugeordneteWirkleistungWechselrichter'].loc[solar_df['operating'] == 1].count(), 0))
    descriptives_dataframe_solar['sum_P_FU'] = float(round(solar_df['ZugeordneteWirkleistungWechselrichter'].loc[solar_df['operating'] == 1].sum(), 0))
    descriptives_dataframe_solar['mean_P_FU'] = float(round(solar_df['ZugeordneteWirkleistungWechselrichter'].loc[solar_df['operating'] == 1].mean(), 1))

    # save as JSON
    os.chdir(path_dict['aggregates_storage'])
    with open(str(file_names_dict['filenames_aggregates_storage']['solar_plants_aggregates']), "w") as outfile:
        json.dump(descriptives_dataframe_solar, outfile)

    # read wind data
    os.chdir(path_dict['static_input'])
    wind_df = pd.read_csv(file_names_dict['filenames_csv_exports_MaStR']['dataframe_wind_file_name_prefix'])
    wind_df = wind_df.drop(wind_df.columns[0], axis=1)

    # get aggregated data from wind dataset
    descriptives_dataframe_wind = {}

    descriptives_dataframe_wind['count_Bruttoleistung'] = float(round(wind_df['Bruttoleistung'].loc[wind_df['operating'] == 1].count(), 0))
    descriptives_dataframe_wind['sum_Bruttoleistung'] = float(round(wind_df['Bruttoleistung'].loc[wind_df['operating'] == 1].sum(), 0))
    descriptives_dataframe_wind['mean_Bruttoleistung'] = float(round(wind_df['Bruttoleistung'].loc[wind_df['operating'] == 1].mean(), 0))

    descriptives_dataframe_wind['count_Nettoleistung'] = float(round(wind_df['Nettonennleistung'].loc[wind_df['operating'] == 1].count(), 0))
    descriptives_dataframe_wind['sum_Nettoleistung'] = float(round(wind_df['Nettonennleistung'].loc[wind_df['operating'] == 1].sum(), 0))
    descriptives_dataframe_wind['mean_Nettoleistung'] = float(round(wind_df['Nettonennleistung'].loc[wind_df['operating'] == 1].mean(), 0))

    descriptives_dataframe_wind

    # save as JSON
    os.chdir(path_dict['aggregates_storage'])
    with open(str(file_names_dict['filenames_aggregates_storage']['wind_plants_aggregates']), "w") as outfile:
        json.dump(descriptives_dataframe_wind, outfile)

def clean_timeshifts(df):
    for c in range(len(df.columns)):
        df.loc['2021-03-28 02:00:00', df.columns[c]] = int(0.5 * (df.loc['2021-03-28 01:00:00', df.columns[c]] + df.loc['2021-03-28 03:00:00', df.columns[c]]))
        df.loc['2021-10-31 02:00:00', df.columns[c]] = int(0.5 * df.loc['2021-10-31 02:00:00', df.columns[c]])
        df.loc['2022-03-27 02:00:00', df.columns[c]] = int(0.5 * (df.loc['2022-03-27 01:00:00', df.columns[c]] + df.loc['2022-03-27 03:00:00', df.columns[c]]))
        df.loc['2022-10-30 02:00:00', df.columns[c]] = int(0.5 * df.loc['2022-10-30 02:00:00', df.columns[c]])

        #generation_historical['2021-03-28':'2021-03-29']           # --> 02:00 --> 0 MWh
        #generation_historical['2021-10-31':'2021-11-01']           # --> 02:00 --> 2*41 MWh
        #generation_historical['2022-03-27':'2022-03-28']           # --> 02:00 --> 0 MWh
        #generation_historical['2022-10-30':'2022-10-31']           # --> 02:00 --> 2*38 MWh

        # decision for further processing: don't correct timeshift loads in this way to keep the correct annual energy amounts

    return df

def preproccess_historical_smard_consumption():
    # switch current folder
    os.chdir(path_dict['smard_historical_data'])

    ## preprocess consumption 2021
    # load 2021 data and change index to timestamp
    consumption_21 = pd.read_csv(file_names_dict['filenames_raw_historical_Smard']['consumption_2021'], delimiter=';', decimal=',')
    consumption_21['time'] = pd.to_datetime(consumption_21['Datum'] + ' ' + consumption_21['Uhrzeit'], format='%d.%m.%Y %H:%M')
    consumption_21.set_index('time', inplace=True)
    consumption_21 = consumption_21.drop(['Datum', 'Uhrzeit'], axis=1)
    # get rid of decimal and thousand characters
    consumption_21['Gesamt (Netzlast)[MWh]'] = [ i.replace(".","") for i in consumption_21['Gesamt (Netzlast)[MWh]'] ]
    consumption_21['Residuallast[MWh]'] = [ i.replace(".","") for i in consumption_21['Residuallast[MWh]'] ]
    consumption_21['Pumpspeicher[MWh]'] = [ i.replace(".","") for i in consumption_21['Pumpspeicher[MWh]'] ]
    # drop missing values
    consumption_21 = consumption_21[consumption_21["Gesamt (Netzlast)[MWh]"].str.contains("-") == False]
    # change to numeric datatype
    consumption_21['Gesamt (Netzlast)[MWh]'] = consumption_21['Gesamt (Netzlast)[MWh]'].astype(int)
    consumption_21['Residuallast[MWh]'] = consumption_21['Residuallast[MWh]'].astype(int)
    consumption_21['Pumpspeicher[MWh]'] = consumption_21['Pumpspeicher[MWh]'].astype(int)
    # transform 15 min values to 1 h
    consumption_21_grouped = consumption_21.resample('H').sum()


    ## preprocess consumption 2022
    # load 2021 data and change index to timestamp
    consumption_22 = pd.read_csv(file_names_dict['filenames_raw_historical_Smard']['consumption_2022'], delimiter=';', decimal=',')
    consumption_22['time'] = pd.to_datetime(consumption_22['Datum'] + ' ' + consumption_22['Uhrzeit'], format='%d.%m.%Y %H:%M')
    consumption_22.set_index('time', inplace=True)
    consumption_22 = consumption_22.drop(['Datum', 'Uhrzeit'], axis=1)
    # get rid of decimal and thousand characters
    consumption_22['Gesamt (Netzlast)[MWh]'] = [ i.replace(".","") for i in consumption_22['Gesamt (Netzlast)[MWh]'] ]
    consumption_22['Residuallast[MWh]'] = [ i.replace(".","") for i in consumption_22['Residuallast[MWh]'] ]
    consumption_22['Pumpspeicher[MWh]'] = [ i.replace(".","") for i in consumption_22['Pumpspeicher[MWh]'] ]
    # drop missing values
    consumption_22 = consumption_22[consumption_22["Gesamt (Netzlast)[MWh]"].str.contains("-") == False]
    # change to numeric datatype
    consumption_22['Gesamt (Netzlast)[MWh]'] = consumption_22['Gesamt (Netzlast)[MWh]'].astype(int)
    consumption_22['Residuallast[MWh]'] = consumption_22['Residuallast[MWh]'].astype(int)
    consumption_22['Pumpspeicher[MWh]'] = consumption_22['Pumpspeicher[MWh]'].astype(int)
    # transform 15 min values to 1 h
    consumption_22_grouped = consumption_22.resample('H').sum()


    # concat to one time serie and delete NaN
    consumption_historical = pd.concat([consumption_21_grouped, consumption_22_grouped], axis=0, ignore_index=False) 


    # clean time shift summertime
    ## this is not performed as it leads to incorrect annnual energy amounts
    #consumption_historical = clean_timeshifts(consumption_historical)

    # check timeshift
    #consumption_historical['2021-03-28':'2021-03-29']           # --> 02:00 --> 0 MWh
    #consumption_historical['2021-10-31':'2021-11-01']           # --> 02:00 --> 2*41 MWh
    #consumption_historical['2022-03-27':'2022-03-28']           # --> 02:00 --> 0 MWh
    #consumption_historical['2022-10-30':'2022-10-31']           # --> 02:00 --> 2*38 MWh

    #consumption_historical['2021-01-01':'2021-12-31'].sum()
    #consumption_historical['2022-01-01':'2022-12-31'].sum()
    # Die Residuallast ist definiert als der Stromverbrauch, abzüglich der Einspeisung von Photovoltaik-, Wind Onshore- und Wind Offshore-Anlagen.

    # save file
    os.chdir(path_dict['preprocessed_time_series'])
    consumption_historical.to_csv(file_names_dict['filenames_csv_exports_historical_Smard']['consumption'])

def preproccess_historical_smard_generation():
    os.chdir(path_dict['smard_historical_data'])

    ## preprocess generation 2021
    # load 2021 data and change index to timestamp
    generation_21 = pd.read_csv(file_names_dict['filenames_raw_historical_Smard']['generation_2021'], delimiter=';', decimal=',')
    generation_21['time'] = pd.to_datetime(generation_21['Datum'] + ' ' + generation_21['Uhrzeit'], format='%d.%m.%Y %H:%M')
    generation_21.set_index('time', inplace=True)
    generation_21 = generation_21.drop(['Datum', 'Uhrzeit'], axis=1)
    generation_21 = generation_21.astype(str)

    # get rid of decimal and thousand characters
    for c in generation_21.columns:
        generation_21.loc[:, c] = [ i.replace(".","") for i in generation_21.loc[:, c] ]

    # drop missing values and set datatype to int
    generation_21 = generation_21[generation_21["Biomasse[MWh]"].str.contains("-") == False]
    generation_21 = generation_21.astype(int)

    # transform 15 min values to 1 h
    generation_21_grouped = generation_21.resample('H').sum()


    ## preprocess generation 2022
    # load 2022 data and change index to timestamp
    generation_22 = pd.read_csv(file_names_dict['filenames_raw_historical_Smard']['generation_2022'], delimiter=';', decimal=',')
    generation_22['time'] = pd.to_datetime(generation_22['Datum'] + ' ' + generation_22['Uhrzeit'], format='%d.%m.%Y %H:%M')
    generation_22.set_index('time', inplace=True)
    generation_22 = generation_22.drop(['Datum', 'Uhrzeit'], axis=1)
    generation_22 = generation_22.astype(str)

    # get rid of decimal and thousand characters
    for c in generation_22.columns:
        generation_22.loc[:, c] = [ i.replace(".","") for i in generation_22.loc[:, c] ]

    # drop missing values and set datatype to int (in gen_22 singularity in "Sonstige Erneuerbare[MWh]")
    generation_22 = generation_22[generation_22["Biomasse[MWh]"].str.contains("-") == False]
    generation_22 = generation_22[generation_22["Sonstige Erneuerbare[MWh]"].str.contains("-") == False]
    generation_22 = generation_22.astype(int)

    # transform 15 min values to 1 h
    generation_22_grouped = generation_22.resample('H').sum()


    # concat to one time serie and delete NaN
    generation_historical = pd.concat([generation_21_grouped, generation_22_grouped], axis=0, ignore_index=False) 

    # clean time shift summertime
    ## this is not performed as it leads to incorrect annnual energy amounts
    #generation_historical = clean_timeshifts(generation_historical)

    # save file
    os.chdir(path_dict['preprocessed_time_series'])
    generation_historical.to_csv(file_names_dict['filenames_csv_exports_historical_Smard']['generation'])

def preproccess_geodata_agg():
    # get lat and lon for each postcode
    os.chdir(path_dict['postcodes_geodata'])
    postcodes_geodata = pd.read_csv(file_names_dict['filenames_raw_postcodes_geodata']['postcode_list'], 
                                    delimiter=",", 
                                    index_col="Postal Code", 
                                    decimal='.', 
                                    encoding='latin-1')
    postcodes_geodata.dropna(inplace=True, how='all')
    postcodes_geodata = postcodes_geodata.reset_index()
    postcodes_geodata.rename(columns = {'Postal Code' : 'Postleitzahl'}, inplace=True)
    postcodes_geodata_lon_lat = postcodes_geodata[['Postleitzahl', 'Latitude', 'Longitude']]

    # get solar power per postcode
    os.chdir(path_dict['static_input'])
    solar_df = pd.read_csv(file_names_dict['filenames_csv_exports_MaStR']['dataframe_solar_file_name_prefix'])
    solar_df = solar_df.drop(solar_df.columns[0], axis=1)
    solar_power_per_postcode = solar_df.groupby('Postleitzahl').agg({'Bruttoleistung': ['sum'], 'Nettonennleistung': ['sum'], 'ZugeordneteWirkleistungWechselrichter': ['sum']})
    solar_power_per_postcode = solar_power_per_postcode.reset_index()
    solar_power_per_postcode = solar_power_per_postcode.merge(postcodes_geodata_lon_lat, on='Postleitzahl', how='left')
    solar_power_per_postcode = solar_power_per_postcode.drop(solar_power_per_postcode.columns[1], axis=1)
    solar_power_per_postcode = solar_power_per_postcode.rename(columns={solar_power_per_postcode.columns[1] : 'sum_Bruttoleistung [kW]', solar_power_per_postcode.columns[2] : 'sum_Nettoleistung [kW]', solar_power_per_postcode.columns[3] : 'sum_ZugeordneteWirkleistungWechselrichter [kW]'})

    # get wind power per postcode
    os.chdir(path_dict['static_input'])
    wind_df = pd.read_csv(file_names_dict['filenames_csv_exports_MaStR']['dataframe_wind_file_name_prefix'])
    wind_df = wind_df.drop(wind_df.columns[0], axis=1)
    wind_power_per_postcode = wind_df.groupby('Postleitzahl').agg({'Bruttoleistung': ['sum'], 'Nettonennleistung': ['sum']})
    wind_power_per_postcode = wind_power_per_postcode.reset_index()
    wind_power_per_postcode = wind_power_per_postcode.merge(postcodes_geodata_lon_lat, on='Postleitzahl', how='left')
    wind_power_per_postcode = wind_power_per_postcode.drop(wind_power_per_postcode.columns[1], axis=1)
    wind_power_per_postcode = wind_power_per_postcode.rename(columns={wind_power_per_postcode.columns[1] : 'sum_Bruttoleistung [kW]', wind_power_per_postcode.columns[2] : 'sum_Nettoleistung [kW]'})

    # outer join on post codes to gather the historic weather data
    postcodes_to_gather_weather_time_series = pd.merge(solar_power_per_postcode['Postleitzahl'], wind_power_per_postcode['Postleitzahl'], how='outer')
    postcodes_to_gather_weather_time_series = postcodes_to_gather_weather_time_series.merge(postcodes_geodata_lon_lat, on='Postleitzahl')
    postcodes_to_gather_weather_time_series = postcodes_to_gather_weather_time_series[['Postleitzahl', 'Latitude', 'Longitude']]


    os.chdir(path_dict['aggregates_storage'])
    solar_power_per_postcode.to_csv(file_names_dict['filenames_csv_postcodes_geodata']['solar_agg_geodata'])
    wind_power_per_postcode.to_csv(file_names_dict['filenames_csv_postcodes_geodata']['wind_agg_geodata'])
    postcodes_to_gather_weather_time_series.to_csv(file_names_dict['filenames_csv_postcodes_geodata']['fetch_weatherdata'])

def fetch_hourly_weatherdata_per_postcode_from_meteostat():
    # catch the hourly weatherdata for the needesd postcodes (7 h duration)
    os.chdir(path_dict['weather_per_postcode_csv'])

    # Set time period
    start = datetime(2021, 1, 1)
    end = datetime(2022, 11, 22)
    standard_altitude = 100

    for p in range(len(postcodes_to_gather_weather_time_series)):

        # Create Point
        fetch_place = Point(postcodes_to_gather_weather_time_series['Latitude'][p], postcodes_to_gather_weather_time_series['Longitude'][p], standard_altitude)

        # Get daily data for 2018
        data = Hourly(fetch_place, start, end)
        data = data.fetch()

        plz_int = postcodes_to_gather_weather_time_series['Postleitzahl'][p].astype(int)
        filename = f'hwd_{str(plz_int)}.csv.xz'
        data.to_csv(filename)
        time.sleep(0.5)

def pivot_features_per_postcode(list_features):
    # feature to build pivoted tables per postcode; we need: ['temp', 'wspd', 'tsun']
    print('starttime: ' + str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())))
    # get available weather profiles
    os.chdir(path_dict['weather_per_postcode_csv'])
    file_list = os.listdir()

    # create index arrays for vectorization
    postcode_array = [ x[4:-7] for x in file_list ]
    #timestamp_array = first_file.index

    for extracted_feature in list_features:
        # rechange to raw data folder after storing the last result from previous loop iteration
        os.chdir(path_dict['weather_per_postcode_csv'])
        # Create an array with the time series of the first postcode
        first_file = pd.read_csv(file_list[0], index_col='time')
        temp_per_postcode_df = first_file[extracted_feature].rename(postcode_array[0])

        # prepare imputation_doc
        imputed_postcodes = {
            'missing_weather_data' : [],
            'imputed_with_data_from_this_postcode' : []
        }

        for p in range(1, len(postcode_array)):
            # process all available weather profiles
            try:
                current_weather_profile = pd.read_csv(file_list[p], index_col='time')
                current_postcode = postcode_array[p]
                current_weather_profile_temp = current_weather_profile[extracted_feature]
                temp_per_postcode_df = pd.concat([temp_per_postcode_df, current_weather_profile_temp.rename(current_postcode)], axis=1)
            except TypeError:
                # set proxy as weather time serie feature from neighbour postcode
                threshold_file_size = 1000
                fn = file_list[p]
                csv_data_path_weather_per_postcode = path_dict['weather_per_postcode_csv']
                if os.path.getsize(f'{csv_data_path_weather_per_postcode}{fn}') < threshold_file_size:
                    print(str(f'{fn} has only a file size of: ') + str(os.path.getsize(f'{csv_data_path_weather_per_postcode}{fn}')) + str(' byte, try to set a proxy'))
                    proxy_setting_successful = 0
                    for s in range(1, 200):
                        file_before = file_list[p-s]
                        file_after = file_list[p+s]
                        sizefile_before = os.path.getsize(f'{csv_data_path_weather_per_postcode}{file_before}')
                        sizefile_after = os.path.getsize(f'{csv_data_path_weather_per_postcode}{file_after}')
                        if sizefile_before > threshold_file_size:
                            try:
                                current_weather_profile = pd.read_csv(file_list[p-s], index_col='time')
                                current_postcode = postcode_array[p]
                                current_weather_profile_temp = current_weather_profile[extracted_feature]
                                temp_per_postcode_df = pd.concat([temp_per_postcode_df, current_weather_profile_temp.rename(current_postcode)], axis=1)
                                
                                imputed_postcodes['missing_weather_data'].append(file_list[p])
                                imputed_postcodes['imputed_with_data_from_this_postcode'].append(file_list[p-s])
                                proxy_setting_successful = 1
                            except TypeError:
                                pass
                        elif sizefile_after > threshold_file_size:
                            try:
                                current_weather_profile = pd.read_csv(file_list[p+s], index_col='time')
                                current_postcode = postcode_array[p]
                                current_weather_profile_temp = current_weather_profile[extracted_feature]
                                temp_per_postcode_df = pd.concat([temp_per_postcode_df, current_weather_profile_temp.rename(current_postcode)], axis=1)
                                
                                imputed_postcodes['missing_weather_data'].append(file_list[p])
                                imputed_postcodes['imputed_with_data_from_this_postcode'].append(file_list[p+s])
                                proxy_setting_successful = 1
                            except TypeError:
                                pass     
                        else:
                            print(str(f's = {s} not enough, try with next radius around filename {fn}'))
                        if proxy_setting_successful == 1:
                            break
                    if proxy_setting_successful == 0:
                        print(str(f'threshold for s in for loop not big enough. Enlarge search radius'))
                    else:
                        print('Proxy was set successful')
                        try:
                            print(f's = {s}')
                        except TypeError:
                            pass  
                else:
                    print('not captured error')
                
        # store data
        os.chdir(path_dict['aggregates_storage_path'])
        feature_key_filenames = f'{extracted_feature}_per_postcode'
        temp_per_postcode_df.to_csv(file_names_dict['filenames_csv_postcodes_weatherdata'][feature_key_filenames])

        # store the imputed postcodes during weather time series preprocessing
        file_sufix = file_names_dict['filenames_aggregates_storage']['weather_data_imputation_sufix']
        file_name_file = f'{extracted_feature}_{file_sufix}'
        with open(file_name_file, "w") as outfile:
            json.dump(imputed_postcodes, outfile)

        try:
            print(f'{file_name_file} processed')
            print('finished this file at: ' + str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())))
        except TypeError:
            pass


    print('endtime: ' + str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())))    

def interpolate_linear_pivot_features_per_postcode(list_features):
    print(str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())) + ' : Started interpolate_linear_pivot_features_per_postcode(list_features) at: ')
    os.chdir(path_dict['aggregates_storage'])
    check_Na_all_features = dict.fromkeys(list_features, 1)
    for extracted_feature in list_features:
        # get dataframe
        feature_key_filenames = f'{extracted_feature}_per_postcode'
        print(str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())) + ' : start loading : ' + str(feature_key_filenames))
        feature_per_postcode_df = pd.read_csv(file_names_dict['filenames_csv_postcodes_weatherdata'][feature_key_filenames], index_col='time')
        
        ## interpolate and check Na values
        # impute within column/time serie
        feature_per_postcode_df = feature_per_postcode_df.interpolate(method='linear', limit_direction='both')
        if any(feature_per_postcode_df[feature_per_postcode_df.columns].isna().sum()) == True:
            # impute additionally rowwise for columns without any values (needed for tsun)
            print(str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())) + ' : do also rowwise imputation')
            feature_per_postcode_df = feature_per_postcode_df.interpolate(method='linear', limit_direction='both', axis=1)
            print(str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())) + ' : finished rowwise imputation')
        else:
            print(str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())) + ' : no rowwise imputation necessary')
        check_Na_all_features[extracted_feature] = any(feature_per_postcode_df[feature_per_postcode_df.columns].isna().sum())   

        # store data
        sufix = file_names_dict['filenames_csv_postcodes_weatherdata']['feature_per_postcode_imputed_sufix']
        feature_key_filenames_imputed = f'{extracted_feature}{sufix}'
        feature_per_postcode_df.to_csv(feature_key_filenames_imputed)
        print(str(time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())) + ' : successful stored')
    # store the verification of the imputation for all processed features
    with open(file_names_dict['filenames_csv_postcodes_weatherdata']['verify_imputation_of_all_features'], "w") as outfile:
        json.dump(check_Na_all_features, outfile)

## ----- previous function bootcampfor processing part -----

def read_aggregate_static_data_MaStR():
    os.chdir(path_dict['aggregates_storage'])
    
    # read solar data
    with open(str(file_names_dict['filenames_aggregates_storage']['solar_plants_aggregates'])) as json_file:
        descriptives_dataframe_solar_reload = json.load(json_file)

    # read wind data
    with open(str(file_names_dict['filenames_aggregates_storage']['wind_plants_aggregates'])) as json_file:
        descriptives_dataframe_wind_reload = json.load(json_file)


    return descriptives_dataframe_solar_reload, descriptives_dataframe_wind_reload

def load_historical_data_smard():
    os.chdir(path_dict['preprocessed_time_series'])

    # read consumption and generation
    consumption_historical = pd.read_csv(file_names_dict['filenames_csv_exports_historical_Smard']['consumption'], index_col=['time'])
    generation_historical = pd.read_csv(file_names_dict['filenames_csv_exports_historical_Smard']['generation'], index_col=['time'])

    return consumption_historical, generation_historical

def load_german_postcodes_with_geodata():
    os.chdir(path_dict['aggregates_storage'])
    postcodes_to_gather_weather_time_series = pd.read_csv(file_names_dict['filenames_csv_postcodes_geodata']['fetch_weatherdata'])
    postcodes_to_gather_weather_time_series = postcodes_to_gather_weather_time_series.drop(columns=[postcodes_to_gather_weather_time_series.columns[0]])

    return postcodes_to_gather_weather_time_series


def load_verify_imputation_of_all_features():
    os.chdir(path_dict['aggregates_storage'])
    with open(str(file_names_dict['filenames_csv_postcodes_weatherdata']['verify_imputation_of_all_features'])) as json_file:
        verify_imputation_of_all_features = json.load(json_file)

    return verify_imputation_of_all_features

def load_MaStR_data_per_postcode():
    os.chdir(path_dict['aggregates_storage'])
    solar_power_per_postcode = pd.read_csv(file_names_dict['filenames_csv_postcodes_geodata']['solar_agg_geodata'])
    wind_power_per_postcode = pd.read_csv(file_names_dict['filenames_csv_postcodes_geodata']['wind_agg_geodata'])
    solar_power_per_postcode = solar_power_per_postcode.drop(solar_power_per_postcode.columns[[0]], axis=1)
    wind_power_per_postcode = wind_power_per_postcode.drop(wind_power_per_postcode.columns[[0]], axis=1)
    
    return solar_power_per_postcode, wind_power_per_postcode

def load_features_per_hour_per_postcode(feature):
    os.chdir(path_dict['aggregates_storage'])
    sufix = file_names_dict['filenames_csv_postcodes_weatherdata']['feature_per_postcode_imputed_sufix']
    feature_key_filenames_imputed = f'{feature}{sufix}'
    data = pd.read_csv(feature_key_filenames_imputed)

    return data