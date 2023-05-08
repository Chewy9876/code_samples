import sys
#for path in sys.path:
#    print(path)

sys.path.append("c:\\Users\\chris\\anaconda3\\envs\\SG2023\\Lib\\site-packages")


import os
#print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
#print("PATH:", os.environ.get('PATH'))

import dash
from dash import Dash, dcc, html, Input, Output         # pip install dash
import dash_bootstrap_components as dbc         # pip install dash_bootstrap_components
import numpy
import pandas as pd
import plotly.express as px


app = dash.Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Home", href="#")),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem("available analysis", header=True),
                dbc.DropdownMenuItem(dbc.NavLink('Overview', href=dash.page_registry['pages.pg_single_energy_vector']['path'], style={'color': 'black'})),
                dbc.DropdownMenuItem(dbc.NavLink('Installed power generation', href=dash.page_registry['pages.pg_installed_power']['path'], style={'color': 'black'})),
                dbc.DropdownMenuItem(dbc.NavLink('Single energy vector', href=dash.page_registry['pages.pg_single_energy_vector']['path'], style={'color': 'black'})),
                                
            ],
            nav=True,
            in_navbar=True,
            label="Charts SMARD.de",
        ),
        #dbc.NavItem(dbc.NavLink("Maps MaStR", href="#"))
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem("available analysis", header=True),
                dbc.DropdownMenuItem(dbc.NavLink('Installed power generation maps', href=dash.page_registry['pages.pg_maps_installed_power_per_postcode']['path'], style={'color': 'black'})),
                                   
            ],
            nav=True,
            in_navbar=True,
            label="Maps MaStR",
        ),
    ],
    brand="Smard Grid Dashboard",
    brand_href="#",
    color="primary",
    dark=True,
)

app.layout = dbc.Container([
    
    navbar,

    html.Hr(),

    dbc.Row(
        [
            dbc.Col(
                [
                    dash.page_container
                ], xs=8, sm=8, md=10, lg=10, xl=10, xxl=12)
        ]
    )
], fluid=True)


if __name__ == "__main__":
    app.run(debug=True)