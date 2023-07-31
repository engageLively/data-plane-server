from dataplane.table_server import *
import os
from glob import glob
files = glob('./data_plane/tables/*.json')
table_server = TableServer()
for filename in files:
    spec = build_table_spec(filename)
    table_server.add_data_plane_table(spec)
pass