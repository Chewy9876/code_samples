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

df_installed_power = cf.load_installed_power_as_df()
df_con_gen = cf.load_historical_gen_and_con_in_one_df()
selection_vectors = list(df_installed_power.columns)
selection_vectors = ['all'] + selection_vectors
layout = html.Div(
    [
        dbc.Row(
            dbc.Col(
                dcc.Markdown('See aggregates of a single raw data time serie')
            )
        ),
        dbc.Row([
            dbc.Col([
                dcc.Dropdown(selection_vectors,
                placeholder="Select a energy vector of installed power",
                id='selected_energy_vector_installed_power',
                maxHeight=600)
            ], width=3),
        ]),
        html.Br(),
        html.Div(id='analytics-output_installed_power'),
    ]
)


@callback(
    Output(component_id='analytics-output_installed_power', component_property='children'),
    Input(component_id='selected_energy_vector_installed_power', component_property='value')
)
def update_energy_vector_selected(input_value):
    if input_value == "all":
        fig_1, df_aggregate_installed_power = cf.plot_all_vectors_installed_power(df_installed_power)
        #fig_2, df_aggregate_con_gen = cf.plot_all_vectors_con_gen_amounts(df_con_gen)
    else:
        # add yearly functionallity later
        input_year = ''
        # create plots
        fig_1 = cf.plot_single_installed_power(df_installed_power, input_value)
        #fig_2 = cf.plot_single_con_gen(df_con_gen, input_value)
        #fig_1 = cf.plot_single_raw_time_serie(df, df.columns.get_loc(input_value))
        #energy_amount_per_year, energy_amount_per_year_per_year_and_month, energy_amount_per_year_per_day, energy_amount_per_week = cf.aggregate_energy_amounts_as_df(df, input_value)
        #fig_2 = cf.plot_aggregates_energy_amounts_per_year(energy_amount_per_year)
        #fig_3 = cf.plot_aggregates_energy_amounts_per_year_and_month(energy_amount_per_year_per_year_and_month)
        #fig_4 = cf.plot_aggregates_energy_amounts_per_day(energy_amount_per_year_per_day)
        #fig_5 = cf.plot_aggregates_energy_amounts_per_week(energy_amount_per_week)
        #heatmap_df_h_wd, input_year = cf.create_df_for_yearly_heatmap_hour_weekday(df, input_value, input_year)
        #fig_6 = cf.plot_timeseries_heatmap_hour_weekday(heatmap_df_h_wd, input_value, input_year)
        #heatmap_df_h_m, input_year = cf.create_df_for_yearly_heatmap_hour_month(df, input_value, input_year)
        #fig_7 = cf.plot_timeseries_heatmap_hours_month(heatmap_df_h_m, input_value, input_year)
        #fig_8, df_ldc_multiple = cf.plot_annual_load_duration_curve_multiple_years(df, input_value)

    page_content = html.Div(children=[
            html.Div(dcc.Graph(id='historical_time_serie_installed_power_raw', figure=fig_1)),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_year', figure=fig_2), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_year_and_month', figure=fig_3), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_week', figure=fig_5), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='historical_time_serie_amounts_per_day', figure=fig_4), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='heatmap_hour_weekday', figure=fig_6), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='heatmap_hour_month', figure=fig_7), style={'width': '49%', 'display': 'inline-block'}),
            #html.Div(dcc.Graph(id='ldc_multiple', figure=fig_8), style={'width': '49%', 'display': 'inline-block'}),
        ])
        
    return page_content