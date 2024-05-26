# BSD 3-Clause License

# Copyright (c) 2019-2021, engageLively
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
Run tests on the dashboard table
'''

import csv
from json import dumps


import pandas as pd
import pytest
from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_STRING, DATA_PLANE_DATE, DATA_PLANE_DATETIME, DATA_PLANE_TIME_OF_DAY, InvalidDataException
from dataplane.data_plane_utils import check_dataplane_type_of_list
from dataplane.data_plane_utils import jsonifiable_value, jsonifiable_column
from dataplane.data_plane_table import DataPlaneTable, RowTable, DataFrameTable, RemoteDataPlaneTable
from pytest_httpserver import HTTPServer

table_test_1 = {
    "rows": [["Ted", 21], ["Alice", 24]],
    "schema": [
        {"name": "name", "type": DATA_PLANE_STRING},
        {"name": "age", "type": DATA_PLANE_NUMBER}
    ]
}

def _makeTable():
    return  DataPlaneTable(table_test_1["schema"], lambda: table_test_1["rows"])

def test_create():
    '''
    Test table creation and ensure that the names and types match
    '''
    table = _makeTable()
    assert table.column_names() == ['name', 'age']
    assert table.column_types() == [DATA_PLANE_STRING, DATA_PLANE_NUMBER]
    assert table.get_rows() == table_test_1["rows"]
    for column in table_test_1["schema"]:
        assert(table.get_column_type(column["name"]) == column["type"])
    assert table.get_column_type(None) == None
    assert table.get_column_type("Foo") == None


def test_all_values_and_range_spec():
    '''
    Test getting all the values and the numeric specification from columns
    '''
    table = _makeTable()
    assert table.all_values('name') == ['Alice', 'Ted']
    assert table.all_values('age') == [21, 24]
    with pytest.raises(InvalidDataException) as e:
        table.all_values(None)
        # assert e.message == 'None is not a column of this table'
    with pytest.raises(InvalidDataException) as e:
        table.all_values('Foo')
        # assert e.message == 'Foo is not a column of this table'
    with pytest.raises(InvalidDataException) as e:
        table.range_spec(None)
        # assert e.message == 'None is not a column of this table'
    with pytest.raises(InvalidDataException) as e:
        table.range_spec('Foo')
        # assert e.message == 'Foo is not a column of this table'
    assert table.range_spec('name') == {'max_val': "Ted", "min_val": "Alice"}
    assert table.range_spec('age') == {'max_val': 24, "min_val": 21}
    table.get_rows = lambda: [['Ted', 21], ['Alice', 24], ['Jane', 20]]
    assert table.range_spec('age') == {'max_val': 24, "min_val": 20}


# Test to build a RowTable

from tests.table_data_good import names, ages, dates, times, datetimes, booleans
rows = [[names[i], ages[i], dates[i], times[i], datetimes[i], booleans[i]] for i in range(len(names))]

schema = [
    {"name": "name", "type": DATA_PLANE_STRING},
    {"name": "age", "type": DATA_PLANE_NUMBER},
    {"name": "date", "type": DATA_PLANE_DATE},
    {"name": "time", "type": DATA_PLANE_TIME_OF_DAY},
    {"name": "datetime", "type": DATA_PLANE_DATETIME},
    {"name": "boolean", "type": DATA_PLANE_BOOLEAN}
]


def test_row_table():
    row_table = RowTable(schema, rows)
    assert (row_table.schema == schema)
    assert (row_table.get_rows() == rows)


#
# test convert to dataframe
#

def test_construct_dataframe():
    row_table = RowTable(schema, rows)
    df = row_table.to_dataframe()
    assert(df.columns.tolist() == row_table.column_names())
    for column in schema:
        column_values = df[column["name"]].tolist()
        assert(check_dataplane_type_of_list(column["type"], column_values))


import requests
from pytest_httpserver import HTTPServer
# @pytest.fixture(scope="session")
# def httpserver_listen_address():
#    return ("127.0.0.1", 8888)

def test_connect():
    httpserver = HTTPServer(port=8888)
    remote_table = RemoteDataPlaneTable('test', schema, httpserver.url_for("/"))
    assert(not remote_table.ok)
    httpserver.expect_request("/get_tables").respond_with_json({"test": schema})
    httpserver.start()
    remote_table.connect_with_server()
    assert(remote_table.ok)
    httpserver.stop()

def test_no_connect():
    httpserver = HTTPServer(port=8888)
    remote_table = RemoteDataPlaneTable('test', schema, httpserver.url_for("/"))
    with pytest.raises(InvalidDataException) as exception:
        remote_table.connect_with_server()
    assert(f'Error connecting with {remote_table.url}/get_tables' in repr(exception))
    assert(not remote_table.ok )
    
def test_bad_connect():
    httpserver = HTTPServer(port=8888)
    remote_table = RemoteDataPlaneTable('test', schema, httpserver.url_for("/"))
    assert(not remote_table.ok)
    httpserver.expect_request("/foobar").respond_with_json({"foo": "bar"})
    httpserver.start()
    with pytest.raises(InvalidDataException) as exception:
        remote_table.connect_with_server()
    assert(f'Bad connection with {remote_table.url}' in repr(exception))
    assert(not remote_table.ok)   
    httpserver.stop()

def test_bad_table():
    httpserver = HTTPServer(port=8888)
    remote_table = RemoteDataPlaneTable('test1', schema, httpserver.url_for("/"))
    assert(not remote_table.ok)
    httpserver.expect_request("/get_tables").respond_with_json({"test": schema})
    httpserver.start()
    with pytest.raises(InvalidDataException) as exception:
        remote_table.connect_with_server()
    assert(f'Server at {remote_table.url} does not have table {remote_table.table_name}' in repr(exception))
    assert(not remote_table.ok)   
    httpserver.stop()

def test_bad_schema():
    httpserver = HTTPServer(port=8888)
    bad_schema = schema[1:]
    remote_table = RemoteDataPlaneTable('test', bad_schema, httpserver.url_for("/"))
    httpserver.expect_request("/get_tables").respond_with_json({"test": schema})
    httpserver.start()
    with pytest.raises(InvalidDataException) as exception:
        remote_table.connect_with_server()
    assert(f' has {len(schema)} columns' in repr(exception))
    assert(not remote_table.ok)
    bad_schema = [dict(entry) for entry in schema]
    bad_schema[0]["name"] = "foo"
    remote_table.schema = bad_schema
    with pytest.raises(InvalidDataException) as exception:
        remote_table.connect_with_server()
    assert('Schema mismatch' in repr(exception))
    bad_schema = [dict(entry) for entry in schema]
    bad_schema[0]["type"] = DATA_PLANE_DATETIME
    remote_table.schema = bad_schema
    with pytest.raises(InvalidDataException) as exception:
        remote_table.connect_with_server()
    assert('Schema mismatch' in repr(exception))
    httpserver.stop()

def test_all_values_and_range_spec():
    httpserver = HTTPServer(port=8888)
    server_table = RowTable(schema, rows)
    
    httpserver.expect_request("/get_tables").respond_with_json({"test": schema})
    all_values_responses = {}

    for column in schema:
        response = server_table.all_values(column["name"])
        json_response = jsonifiable_column(response, column["type"])
        httpserver.expect_request("/get_all_values", query_string={"table_name": "test", "column_name": column["name"]}).respond_with_json(json_response)
        all_values_responses[column["name"]] = response
    range_spec_responses = {}
    for column in schema:
        response = server_table.range_spec(column["name"])
        json_response = {
            "max_val": jsonifiable_value(response["max_val"], column["type"]),
            "min_val": jsonifiable_value(response["min_val"], column["type"]),
        }
        httpserver.expect_request("/get_range_spec", query_string={"table_name": "test", "column_name": column["name"]}).respond_with_json(json_response)
        range_spec_responses[column["name"]] = response
    httpserver.start()
    remote_table = RemoteDataPlaneTable('test', schema, httpserver.url_for("/"))
    for column in schema:
        assert(remote_table.all_values(column["name"]) == all_values_responses[column["name"]])
        assert(remote_table.range_spec(column["name"]) == range_spec_responses[column["name"]])
    httpserver.stop()

