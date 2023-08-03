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

import pytest
import json
from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_STRING, DATA_PLANE_DATE, DATA_PLANE_DATETIME, DATA_PLANE_TIME_OF_DAY, InvalidDataException
from dataplane.data_plane_table import DataPlaneFilter, DataPlaneTable, RowTable, check_valid_spec, DATA_PLANE_FILTER_FIELDS, DATA_PLANE_FILTER_OPERATORS

import os

os.chdir('/workspaces/dataplane/data_plane')
from app import app

client = app.test_client()

UNPROTECTED_SPEC = {
    "unprotected": [
        {"name": "column1", "type": "string"},
        {"name": "column2", "type": "number"}
    ],
    "test1": [
        { "name": "name", "type": "string" },
        { "name": "age",  "type": "number" },
        { "name": "date", "type": "date" },
        { "name": "time",  "type": "timeofday"},
        { "name": "datetime", "type": "datetime" },
        { "name": "boolean", "type": "boolean" }
    ]

}

PROTECTED_SPEC = UNPROTECTED_SPEC.copy()
PROTECTED_SPEC["protected"] = PROTECTED_SPEC["unprotected"]

unprotected_tables = ['unprotected', 'test1']

def test_get_table_spec():
    response = client.get('/init')
    response = client.get('/get_table_spec')
    assert response.status_code == 200
    # result = json.loads(response.json)
    assert response.json == {
        "protected": ["foo"],
        "test1": [],
        "unprotected": []
    }
    header_list_and_response = [
        ({}, UNPROTECTED_SPEC),
        ({"foo":"foo"}, UNPROTECTED_SPEC),
        ({"foo": "bar"}, PROTECTED_SPEC)
    ]
    for (headers, result) in header_list_and_response:
        response = client.get('/get_tables', headers = headers)
        assert response.status_code == 200
        assert response.json == result

def test_all_values_and_range_spec():
    # For get_all_values and get_range_spec, just check the response codes -- 
    # we know the values from testing the table server
    routes = ['get_range_spec', 'get_all_values']
    header_list = [({}, 403), ({"foo": "foo"}, 403), ({"foo": "bar"}, 200) ]
    
    for route in routes:
        # don't provide required arguments
        for suffix in ['', '?table_name', '?column_name']:
            response = client.get(f'{route}{suffix}')
            assert response.status_code == 400
        # pass a bad table name
        response = client.get(f'{route}?table_name=foo&column_name=bar')
        assert response.status_code == 400
        # pass a bad column  name
        response = client.get(f'{route}?table_name=unprotected&column_name=bar')
        assert response.status_code == 400
        # check authorization
        for (header, code) in header_list:
            response = client.get(f'{route}?table_name=protected&column_name=column1', headers = header)
            assert response.status_code == code
        # Make sure unprotected is OK
        response = client.get(f'{route}?table_name=unprotected&column_name=column1')
        assert response.status_code == 200
        results = {
            'get_all_values': [ "Alexandra", "Hitomi", "Karen", "Sujata", "Tammy", "Tori"],
            'get_range_spec': {'max_val': 'Tori', 'min_val': 'Alexandra'}
        }
        assert response.json == results[route]

def test_get_filtered_rows():
    # Check get_filtered_rows
    # Check for a bad table name
    response = client.post('get_filtered_rows')
    assert response.status_code == 400
    response = client.get('get_filtered_rows', json={'table': 'foo'})
    assert response.status_code == 400
    # Missing authentication
    for (header, code) in header_list:
        response = client.get('get_filtered_rows', json={'table_name':'protected'}, headers=header)
        assert response.status_code == code
    # Bad filter

    



    



    
    
    
    

