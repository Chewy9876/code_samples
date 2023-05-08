'''
import dash
from dash import dcc, html, callback, Output, Input
import plotly.express as px
import dash_bootstrap_components as dbc

dash.register_page(__name__, name='Home') # '/' is home page

layout = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Markdown('test string 1',
                                     id='test_str_1')
                    ], xs=10, sm=10, md=8, lg=4, xl=4, xxl=4
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Markdown('test string 2',
                                     id='test_str_2')
                    ], xs=10, sm=10, md=8, lg=4, xl=4, xxl=4
                )
            ]
        )
    ]
)
'''

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc

import common_namespace_SG as cn
import common_functions_SG as cf

import numpy as np
import pandas as pd
import os
from os.path import exists
import json
import time
#from datetime import datetime as dt
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
import geopandas

# Import Meteostat library and dependencies
import matplotlib.pyplot as plt
#!pip install meteostat
from meteostat import Point, Daily, Hourly


# Create names space

path_dict = cn.create_path_names_dict()                        # Create pathnames
file_names_dict = cn.create_file_names_dict()                  # Create filenames
model_dict = cn.create_model_dict()                            # Create model parameters

# Start app

dash.register_page(__name__)

# load geodata information of Germany
#gdf, gdff, gdf_transformed = cf.load_geojson()
selection_vectors = [
    #str(file_names_dict['filenames_raw']['prefix_solar_unity']),
    str(file_names_dict['filenames_raw']['prefix_wind_unity']),
    str(file_names_dict['filenames_raw']['prefix_biomass_unity']),
    ##str(file_names_dict['filenames_raw']['prefix_ee_other_unity']),
    str(file_names_dict['filenames_raw']['prefix_nuclear_generator_unity']),
    str(file_names_dict['filenames_raw']['prefix_power_consumer_unity']),
    str(file_names_dict['filenames_raw']['prefix_combustion_unity']),
    str(file_names_dict['filenames_raw']['prefix_water_generator_unity']),
    #str(file_names_dict['filenames_raw']['prefix_storage_unity']),
    str(file_names_dict['filenames_raw']['prefix_gas_generator_unity']),
    str(file_names_dict['filenames_raw']['prefix_gas_storage_unity']),
    str(file_names_dict['filenames_raw']['prefix_gas_consumer_unity']),
]
#df_installed_power = cf.load_installed_power_as_df()
#df_con_gen = cf.load_historical_gen_and_con_in_one_df()
#selection_vectors = list(df_installed_power.columns)
#selection_vectors = ['all'] + selection_vectors
layout = html.Div(
    [
        dbc.Row(
            dbc.Col(
                dcc.Markdown('See maps of installed power per energy vector')
            )
        ),
        dbc.Row([
            dbc.Col([
                dcc.Dropdown(selection_vectors,
                placeholder="Select a energy vector of installed power",
                id='selected_energy_vector_installed_power_for_maps',
                maxHeight=600)
            ], width=3),
        ]),
        html.Br(),
        html.Div(id='analytics-output_installed_power_maps'),
    ]
)


@callback(
    Output(component_id='analytics-output_installed_power_maps', component_property='children'),
    Input(component_id='selected_energy_vector_installed_power_for_maps', component_property='value')
)
def update_energy_vector_selected(input_value):

    df_MaStR = cf.load_unity_extractions_from_MaStR(input_value)
    class_features = 0
    aggregation_feature = model_dict['aggregation_feature_per_postcode_classes_for_installed_power'][input_value][class_features]
    df_MaStR_postcode_aggregates = cf.aggregate_MaStR_extraction_per_postcode(df_MaStR, input_value, aggregation_feature)
    gdf, gdff, gdf_transformed = cf.load_geojson()
    use_geopandas = 0
    if use_geopandas:
        merged_data = pd.merge(gdf, df_MaStR_postcode_aggregates, left_on='postcode', right_on='Postleitzahl', how='left')
        fig_1 = merged_data.explore(aggregation_feature, legend='True')
    
    use_plotly_choropleth_map = 1
    if use_plotly_choropleth_map:
        os.chdir(path_dict['postcodes_geodata'])
        filename = 'postleitzahlen.geojson'
        gdf = geopandas.read_file(filename)
        #gdf.to_file('main_map.geojson', driver='GeoJSON')
        gdf_transformed.to_file('main_map.geojson', driver='GeoJSON')
        with open ('main_map.geojson', 'r') as infile:
            map_json = json.load(infile)
        fig_1 = px.choropleth_mapbox(df_MaStR_postcode_aggregates, geojson=map_json,
                                locations='Postleitzahl', featureidkey='properties.postcode',
                                color=aggregation_feature,
                                )
    page_content = html.Div(children=[
            html.Div(dcc.Graph(id='maps_installed_power_raw', figure=fig_1)),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_year', figure=fig_2), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_year_and_month', figure=fig_3), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_week', figure=fig_5), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_day', figure=fig_4), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='heatmap_hour_weekday', figure=fig_6), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='heatmap_hour_month', figure=fig_7), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='ldc_multiple', figure=fig_8), style={'width': '49%', 'display': 'inline-block'}),
        ])
        
    return page_content