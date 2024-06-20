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
Run tests on the table server, the middleware that sits between the data plane structures
and the data plane server
'''
import pytest
from data_plane_server.table_server import TableServer, TableNotFoundException, TableNotAuthorizedException, ColumnNotFoundException, build_table_spec, Table
from dataplane.data_plane_table import RowTable
from dataplane.data_plane_utils import InvalidDataException, DATA_PLANE_STRING
import os
os.chdir('/workspaces/dataplane/data_plane')

def test_build_table_spec():
    '''
    Test build_table_spec.  We have three table files in the right format sitting in the 
    tables directory.  Just make sure it builds the right row tables and extracts the 
    name and the headers
    '''
    spec = build_table_spec('./tables/protected.json')
    assert spec["name"] == 'protected'
    assert spec['table'].header_dict == {'foo': 'bar'}
    assert spec['table'].table.schema == [
        {"name": "column1", "type": "string"},
        {"name": "column2", "type": "number"}
    ]
    assert spec['table'].table.rows == [
        ["Tom", 23],
        ["Misha", 37],
        ["Karen", 38],
        ["Vijay", 27],
        ["Alexandra", 25],
        ["Hitomi", 45]
    ]
    spec = build_table_spec('./tables/unprotected.json')
    assert spec["name"] == 'unprotected'
    assert spec['table'].header_dict == {}
    assert spec['table'].table.schema == [
        {"name": "column1", "type": "string"},
        {"name": "column2", "type": "number"}
    ]
    assert spec['table'].table.rows == [
        ["Tammy", 48],
        ["Sujata", 36],
        ["Karen", 38],
        ["Tori", 27],
        ["Alexandra", 25],
        ["Hitomi", 45]
    ]

def _check_ok(table, dataplane_table, headers):
    # Utility for test_table() -- just make sure the table is right.
    assert table.table == dataplane_table
    assert table.header_dict == headers

def test_table():
    # test the Table() constructor.  Most of the correct cases were tested
    # by test_build_table
    # Simple table for the tests
    schema = [{"name": 'name', 'type':  DATA_PLANE_STRING}]
    rows = [['Tom'], ['Dick']]
    table = RowTable(schema, rows)
    # Test a bad table type
    with pytest.raises(InvalidDataException) as err:
        Table(None)
    with pytest.raises(InvalidDataException) as err:
        Table(1, {})
    # Test OK with headers missing and None (only cases not covered by test_build_table)
    _check_ok(Table(table), table, {})
    _check_ok(Table(table, None), table, {})
    # Test a bad type for headers
    with pytest.raises(InvalidDataException) as err:
        Table(table, 1)
    # Test a bad type for header keys
    with pytest.raises(InvalidDataException) as err:
        Table(table, {1: "foo"})
    with pytest.raises(InvalidDataException) as err:
        Table(table, {None: "foo"})
    # Test bad types for header value fields
    with pytest.raises(InvalidDataException) as err:
        Table(table, {"foo": None})
    with pytest.raises(InvalidDataException) as err:
        Table(table, {"foo": (3, 2)})


# Set up the table server for the remaining tests, and add the test tables to it

table_server = TableServer()
files = ['protected.json', 'unprotected.json', 'test1.json']
specs = {}
for file in files:
    spec = build_table_spec(f'./tables/{file}')
    table_server.add_data_plane_table(spec)
    specs[spec["name"]] = spec["table"]


def test_add_data_plane_table():
    # Test to make sure the tables were all added properly
    assert table_server.servers == specs

dict_expected_unprotected = {}
dict_expected_protected = {}

# Set up the table dictionaries for the unprotected case (no or bad headers) and
# the protected case (correct headers for the protected table present)
for name in ['unprotected', 'test1']:
    dict_expected_unprotected[name] = specs[name].table.schema
    dict_expected_protected[name] = specs[name].table.schema

dict_expected_protected['protected'] = specs['protected'].table.schema

def test_get_table_dictionary():
    # test that we get the table dictionaries out
    assert table_server.get_table_dictionary() == dict_expected_unprotected
    assert table_server.get_table_dictionary(None) == dict_expected_unprotected
    assert table_server.get_table_dictionary({'bar': 2}) == dict_expected_unprotected
    assert table_server.get_table_dictionary({'foo': 2}) == dict_expected_unprotected
    assert table_server.get_table_dictionary({'foo': 'bar'}) == dict_expected_protected


def test_get_auth_spec():
    # Test that the authentication specs are returned properly
    assert table_server.get_auth_spec() == {
        'protected': ['foo'],
        'unprotected': [],
        'test1': []
    }


def test_get_table():
    # Test getting tables, including all corner cases
    # Table -- None and bad tpe
    with pytest.raises(TableNotFoundException) as err:
        table_server.get_table(None)
    with pytest.raises(TableNotFoundException) as err:
        table_server.get_table('foo')
    # Test variants in headers: for the protected case, no headers, empty headers,
    # headers = None, headers with incorrect key, headfers with incorrect value
    headers = [{}, None, {'foo': 2}, {'bar': 'bar'}]
    with pytest.raises(TableNotAuthorizedException) as err:
        table_server.get_table('protected')
    for header in headers:
        with pytest.raises(TableNotAuthorizedException) as err:
            table_server.get_table('protected', header)
    # When the header contains the right key and value, the value is returned, and make
    # sure it's right
    assert table_server.get_table('protected', {"foo": "bar"}) == specs['protected'].table
    # Same tests for the unprotected tables, except that the TableNotAuthorizedException cases
    # are now valid returns
    for table_name in ['unprotected', 'test1']:
        assert table_server.get_table(table_name) == specs[table_name].table
        for header in headers:
            assert table_server.get_table(table_name, header) == specs[table_name].table
        assert table_server.get_table(table_name, {"foo": "bar"}) == specs[table_name].table

def test_get_all_values():
    # Since self.get_table() is the first line in self.get_all_values(), we can skip all the table 
    # tests
    # bad column nsme
    for column_name in [None, 1, (3, 2)]:
        with pytest.raises(AssertionError) as err:
            table_server.get_all_values('protected', column_name)
    # column not found
    with pytest.raises(ColumnNotFoundException) as err:
        table_server.get_all_values('protected', 'foo', {'foo': 'bar'})
    
    # Check the good cases
    assert table_server.get_all_values('protected', 'column2', {'foo': 'bar'}) == specs['protected'].table.all_values('column2')

def test_get_range_spec():
    # the code for get_range_spec and get_all_values are essentially the same
    for column_name in [None, 1, (3, 2)]:
        with pytest.raises(AssertionError) as err:
            table_server.get_range_spec('protected', column_name)
    # column not found
    with pytest.raises(ColumnNotFoundException) as err:
        table_server.get_range_spec('protected', 'foo', {'foo': 'bar'})
    
    # Check the good cases
    assert table_server.get_range_spec('protected', 'column2', {'foo': 'bar'}) == specs['protected'].table.range_spec('column2')

test_table()

    


        