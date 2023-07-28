'''
A framework to easily and quickly implement a web server which serves tables according to
the Data Plane Rest  protocol.  This implements the URL methods get_filtered_rows, get_all_values,
and get_numeric_spec.  It parses the arguments, checking for errors, takes the
table argument, looks up the appropriate DataPlaneTable to serve for that table, and
then calls the method on that server to serve the request.  If no exception is thrown,
returns a 200 with the result as a JSON structure, and if an exception is thrown, returns
a 400 with an approrpriate error message.
All of the methods here except for add_data_plane_table are simply route targets: none are
designed for calls from any method other than flask.
The way to use this is very simple:
1. For each Table to be served, create an instance of data_plane_table.DataPlaneTable
2. Call add_data_plane_table(table_name, data_plane_table)
After that, requests for the named table will be served by the created data server.

'''

# BSD 3-Clause License

# Copyright (c) 2023, engageLively
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


import csv

import pandas as pd

from data_plane_utils import DATA_PLANE_SCHEMA_TYPES
from data_plane_utils import InvalidDataException
from data_plane_table import DataPlaneTable, RowTable
from data_plane_server import add_data_plane_table
from conversion_utils import convert_row


def create_server_from_csv(table_name, path_to_csv_file):
    '''
    Create a server from a CSV file.The file must meet the format for a RowTable:
    1. Each row must contain the same number of columns;
    2. The first row (row 0) are the names of the columns
    3. The second row (row 1)  has the types of the columns
    4. The type of each entry in rows 2-n must match the declared type of the column
    Note that it is expected that the csv file will have been appropriately conditioned; all
    of the elements in each numeric column are numbers, and dates, times, and datetimes are in isoformat
    '''
    try:
        with open(path_to_csv_file, 'r') as f:
            r = csv.reader(f)
            rows = r.readrows()
        assert len(rows) > 2
        num_columns = len(rows[0])
        for row in rows[1:]: assert len(row) == num_columns
        for entry in rows[1]: assert entry in DATA_PLANE_SCHEMA_TYPES
    except Exception as error:
        raise InvalidDataException(error)
    
    schema = [{"name": rows[0][i], "type": rows[1][i]} for i in range(num_columns)]
    column_type_list = [{"type": data_plane_type} for data_plane_type in rows[1]]
    try:
        final_rows = [convert_row( row, column_type_list) for row in rows[2:]]
        server = RowTable(schema, final_rows)
        add_data_plane_table(table_name, server)

    except ValueError as error:
        raise InvalidDataException(f'{error} raised during type conversion')
    
class RemoteCSVTable(DataPlaneTable):
    '''
    A very common format for data interchange on the Internet is a downloadable
    CSV file.  It's so common it's worth making a class, just for this.  The
    idea is that, when a get_rows request comes in, we download the table
    into a dataframe and then return the list of rows, perhaps after doing
    unit conversion
    '''

    def __init__(self, schema, url):
        super(schema, self.get_rows)
        self.url = url
        self.dataframe = None

    def get_rows(self):
        if self.dataFrame is None:
            self.dataframe = pd.read_csv(self.url)
        return self.dataframe.values.tolist()
    