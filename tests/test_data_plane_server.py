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
    response = client.get('/init')
    
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

        # Make sure that dates, times, and datetimes jsonify properly
        results = {
            'get_all_values': {
                'date': [     "2000-03-01",  "2000-07-05", "2000-12-26", "2001-01-07", "2001-01-22", "2001-03-05", "2001-04-14", "2001-11-28", "2002-06-11", "2003-02-23", "2003-04-09", "2003-05-29", "2003-08-27", "2003-09-09", "2003-12-17", "2004-03-05", "2004-04-21", "2004-05-23", "2004-08-31", "2004-09-28", "2004-12-14", "2005-04-01", "2005-05-27", "2005-06-08", "2005-07-25", "2005-07-26", "2006-01-04", "2006-02-02", "2006-07-11", "2006-11-11", "2007-08-05", "2007-10-03", "2007-10-09", "2007-11-11", "2008-01-04", "2008-04-19", "2008-04-22", "2009-07-23", "2010-03-19", "2010-04-24", "2010-06-28", "2010-08-07", "2010-09-23", "2010-10-02", "2010-12-10", "2010-12-24", "2011-03-27", "2011-12-09", "2012-10-13", "2013-06-20", "2013-07-04", "2013-10-03", "2013-10-10", "2013-10-12", "2013-10-15", "2014-01-06", "2014-04-29", "2014-05-21", "2014-08-13", "2014-08-15", "2015-03-05", "2015-04-05", "2015-05-17", "2015-06-16", "2015-10-28", "2016-01-18", "2016-05-26", "2016-07-23", "2016-08-27", "2016-12-14", "2017-05-23", "2017-07-01", "2017-07-22", "2017-11-19", "2017-12-17", "2018-03-10", "2018-03-11", "2018-12-01", "2019-03-23", "2019-06-08", "2019-07-26", "2019-09-03", "2019-09-28", "2019-12-17", "2020-02-11", "2020-04-02", "2020-06-29", "2020-07-22", "2020-09-24", "2021-02-10", "2021-04-21", "2021-06-11", "2021-06-22", "2021-10-02", "2021-10-17", "2022-04-01", "2022-05-28", "2022-07-08", "2022-10-24", "2023-01-24"],'time': ["00:11:37", "00:13:53", "00:16:33", "00:24:01", "00:37:44", "00:44:46", "00:50:14", "00:53:03", "01:06:34", "01:46:17", "01:56:52", "03:13:27", "03:20:46", "03:24:24", "03:35:48", "04:07:44", "04:34:48", "04:39:45", "04:40:59", "05:20:58", "06:11:21", "06:38:17", "06:58:34", "07:09:58", "07:27:51", "07:33:45", "07:39:00", "08:06:15", "08:09:09", "08:19:40", "08:58:06", "09:15:21", "09:23:28", "10:07:34", "10:09:12", "10:43:15", "10:48:05", "10:51:03", "10:57:56", "11:16:57", "11:24:55", "11:29:53", "11:52:44", "12:03:04", "12:19:43", "12:33:26", "12:36:15", "13:24:37", "13:40:27", "14:06:35", "14:07:44", "14:25:31", "14:29:36", "14:40:38", "14:41:46", "15:09:38", "15:24:08", "15:53:20", "16:01:11", "16:09:03", "16:14:38", "16:42:31", "16:55:20", "17:12:00", "17:13:02", "17:30:14", "17:35:48", "17:38:14", "17:42:39", "17:47:27", "17:53:11", "18:14:10", "18:28:49", "18:35:37", "18:36:56", "18:54:22", "19:19:52", "19:58:11", "20:03:52", "20:21:12", "20:21:17", "20:24:01", "20:25:31", "20:55:33", "21:15:48", "21:23:29", "21:35:58", "21:46:58", "21:59:19", "22:15:59", "22:17:08", "22:17:17", "22:20:04", "22:28:52", "22:40:39", "23:11:13", "23:15:20", "23:15:27", "23:35:29", "23:44:12" ],
                'datetime': ["2000-03-01T19:58:11", "2000-07-05T14:40:38", "2000-12-26T01:06:34", "2001-01-07T17:38:14", "2001-01-22T16:55:20", "2001-03-05T07:27:51", "2001-04-14T08:19:40", "2001-11-28T13:24:37", "2002-06-11T10:09:12", "2003-02-23T14:41:46", "2003-04-09T21:35:58", "2003-05-29T12:03:04", "2003-08-27T15:53:20", "2003-09-09T07:39:00", "2003-12-17T23:15:20", "2004-03-05T08:06:15", "2004-04-21T18:54:22", "2004-05-23T16:01:11", "2004-08-31T03:13:27", "2004-09-28T18:28:49", "2004-12-14T18:36:56", "2005-04-01T00:24:01", "2005-05-27T17:35:48", "2005-06-08T04:07:44", "2005-07-25T07:33:45", "2005-07-26T20:21:17", "2006-01-04T10:07:34", "2006-02-02T20:24:01", "2006-07-11T14:29:36", "2006-11-11T21:59:19", "2007-08-05T16:09:03", "2007-10-03T14:07:44", "2007-10-09T21:23:29", "2007-11-11T20:21:12", "2008-01-04T04:34:48", "2008-04-19T20:25:31", "2008-04-22T09:15:21", "2009-07-23T06:58:34", "2010-03-19T04:40:59", "2010-04-24T03:35:48", "2010-06-28T06:38:17", "2010-08-07T23:44:12", "2010-09-23T18:35:37", "2010-10-02T22:15:59", "2010-12-10T08:09:09", "2010-12-24T11:29:53", "2011-03-27T10:51:03", "2011-12-09T13:40:27", "2012-10-13T19:19:52", "2013-06-20T00:53:03", "2013-07-04T10:48:05", "2013-10-03T00:50:14", "2013-10-10T17:53:11", "2013-10-12T15:09:38", "2013-10-15T23:11:13", "2014-01-06T17:42:39", "2014-04-29T00:44:46", "2014-05-21T17:47:27", "2014-08-13T12:33:26", "2014-08-15T18:14:10", "2015-03-05T22:40:39", "2015-04-05T01:56:52", "2015-05-17T14:25:31", "2015-06-16T01:46:17", "2015-10-28T10:43:15", "2016-01-18T00:11:37", "2016-05-26T00:16:33", "2016-07-23T06:11:21", "2016-08-27T09:23:28", "2016-12-14T17:30:14", "2017-05-23T20:55:33", "2017-07-01T10:57:56", "2017-07-22T08:58:06", "2017-11-19T15:24:08", "2017-12-17T23:35:29", "2018-03-10T22:28:52", "2018-03-11T00:13:53", "2018-12-01T20:03:52", "2019-03-23T14:06:35", "2019-06-08T16:42:31", "2019-07-26T03:20:46", "2019-09-03T05:20:58", "2019-09-28T22:17:08", "2019-12-17T12:19:43", "2020-02-11T04:39:45", "2020-04-02T22:17:17", "2020-06-29T21:15:48", "2020-07-22T11:16:57", "2020-09-24T11:24:55", "2021-02-10T03:24:24", "2021-04-21T23:15:27", "2021-06-11T00:37:44", "2021-06-22T21:46:58", "2021-10-02T22:20:04", "2021-10-17T17:13:02", "2022-04-01T11:52:44", "2022-05-28T07:09:58", "2022-07-08T17:12:00", "2022-10-24T12:36:15", "2023-01-24T16:14:38"]
            },
            'get_range_spec': {
                'time': {
                    "max_val": "23:44:12",
                    "min_val": "00:11:37"
                },
                'date': {
                    "max_val": "2023-01-24",
                    "min_val": "2000-03-01"
                },
                'datetime': {
                    "max_val": "2023-01-24T16:14:38",
                    "min_val": "2000-03-01T19:58:11"
                }
            }
        }
        for column in ['time', 'date', 'datetime']:
            response = client.get(f'{route}?table_name=test1&column_name={column}')
            assert response.status_code == 200
            assert response.json == results[route][column]

def test_get_filtered_rows():
    # Check get_filtered_rows
    # Check for a bad table name
    response = client.get('/init')
    response = client.post('/get_filtered_rows', json={})
    assert response.status_code == 400
    response = client.post('get_filtered_rows', json={'table': 'foo'})
    assert response.status_code == 400
    header_list = [({}, 403), ({"foo": "foo"}, 403), ({"foo": "bar"}, 200) ]
    # Missing authentication
    for (header, code) in header_list:
        response = client.post('get_filtered_rows', json={'table':'protected'}, headers=header)
        assert response.status_code == code
    # Bad column
    response = client.post('get_filtered_rows', json=({"table": "test1", "columns": "foo"}))
    assert response.status_code == 400
    response = client.post('get_filtered_rows', json=({"table": "test1", "columns": ["foo"]}))
    assert response.status_code == 400

    # Bad filter spec.  Note that the various cases of a bad filter spec are tested in
    # test_data_plane, so here we just look for a 400
    response = client.post('get_filtered_rows', json=({"table": "test1", "filter": "foo"}))
    assert response.status_code == 400

    # Test for a good result with column selection
    result =  [["Jenine" ], ["Debi" ], ["Catherina" ], ["Doloritas" ], ["Deena"]]
    filter_spec = {"operator": "IN_RANGE", "column": "age", "max_val": 45, "min_val": 40}
    response = client.post('get_filtered_rows', json={"table": "test1", "columns": ["name"], "filter": filter_spec})
    assert response.status_code == 200
    assert response.json == result

    # Make sure datetimes jsonify correctly
    result =  [["2017-07-01"], ["2001-04-14" ], ["2016-01-18" ], ["2015-05-17" ], ["2004-05-23"]]
    response = client.post('get_filtered_rows', json={"table": "test1", "columns": ["date"], "filter": filter_spec})
    assert response.status_code == 200
    assert response.json == result

    result =  [["2017-07-01"], ["2001-04-14" ], ["2016-01-18" ], ["2015-05-17" ], ["2004-05-23"]]
    response = client.post('get_filtered_rows', json={"table": "test1", "columns": ["date"], "filter": filter_spec})
    assert response.status_code == 200
    assert response.json == result
    
    result = [["10:57:56"  ],  ["08:19:40"  ],  [ "00:11:37"  ],  [ "14:25:31"  ],  [ "16:01:11"  ]]
    response = client.post('get_filtered_rows', json={"table": "test1", "columns": ["time"], "filter": filter_spec})
    assert response.status_code == 200
    assert response.json == result
  
  
    result = [["2017-07-01T10:57:56"], ["2001-04-14T08:19:40"], ["2016-01-18T00:11:37"], ["2015-05-17T14:25:31"], ["2004-05-23T16:01:11"]]
    response = client.post('get_filtered_rows', json={"table": "test1", "columns": ["datetime"], "filter": filter_spec})
    assert response.status_code == 200
    assert response.json == result
