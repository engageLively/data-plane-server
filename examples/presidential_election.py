# BSD 3-Clause License

# Copyright (c) 2019-2022, engageLively
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.

# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.

# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

'''
A simple example application showing how to use the DataPlaneServer to serve a number of
political and economic tables.
'''
import sys

# we're in a peer directory, so add the parent to the PYTHONPATH
sys.path.append('..')

from flask import Flask
from flask_cors import CORS
from data_plane_server.data_plane_server import data_plane_server_blueprint, table_server
from data_plane_server.data_plane_csv_server import create_server_from_csv
from dataplane.data_plane_utils import DATA_PLANE_DATE, DATA_PLANE_NUMBER

app = Flask(__name__)
CORS(app)
app.register_blueprint(data_plane_server_blueprint)

tables = [
    {'name': 'electoral_college', 'path': 'data/electoral_college.csv'},
    {'name': 'nationwide_vote', 'path': 'data/nationwide_vote.csv'},
    {'name': 'presidential_vote', 'path': 'data/presidential_vote.csv'},
    {'name': 'presidential_margins', 'path': 'data/presidential_margins.csv'},
    {'name': 'presidential_vote_history', 'path': 'data/presidential_vote_history.csv'},
]
#
# Create the tables with the spec and register them with the framework
#
for table_spec in tables:
    create_server_from_csv(table_spec["name"], table_spec["path"], table_server, {"Content-Type": "application/json"})

remote_tables = {
    'median_income': {'schema': [{'name': 'Year', 'type': DATA_PLANE_DATE}, {'name': 'income', 'type': DATA_PLANE_NUMBER}], 'url': 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=MEPAINUSA646N&scale=left&cosd=1974-01-01&coed=2022-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-06-09&revision_date=2024-06-09&nd=1974-01-01'},
    'median_real_income': {'schema': [{'name': 'Year', 'type': DATA_PLANE_DATE}, {'name': 'income', 'type': DATA_PLANE_NUMBER}], 'url': 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=MEHOINUSA672N&scale=left&cosd=1984-01-01&coed=2022-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-06-10&revision_date=2024-06-10&nd=1984-01-01'},
    'real_gdp':  {'schema': [{'name': 'Year', 'type': DATA_PLANE_DATE}, {'name': 'income', 'type': DATA_PLANE_NUMBER}],'url': 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=MEHOINUSA672N&scale=left&cosd=1984-01-01&coed=2022-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-06-10&revision_date=2024-06-10&nd=1984-01-01'},
    'debt_pct_gdp':  {'schema': [{'name': 'Year', 'type': DATA_PLANE_DATE}, {'name': 'income', 'type': DATA_PLANE_NUMBER}],'url': 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GFDEGDQ188S&scale=left&cosd=1966-01-01&coed=2024-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-06-10&revision_date=2024-06-10&nd=1966-01-01'},
    'consumer_price_index':  {'schema': [{'name': 'Year', 'type': DATA_PLANE_DATE}, {'name': 'income', 'type': DATA_PLANE_NUMBER}],'url': 'https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=FPCPITOTLZGUSA&scale=left&cosd=1960-01-01&coed=2022-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Annual&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-06-10&revision_date=2024-06-10&nd=1960-01-01'},
}

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
