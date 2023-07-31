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
from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_STRING, DATA_PLANE_DATE, DATA_PLANE_DATETIME, DATA_PLANE_TIME_OF_DAY, InvalidDataException
from dataplane.data_plane_table import DataPlaneFilter, DataPlaneTable, RowTable, check_valid_spec, DATA_PLANE_FILTER_FIELDS, DATA_PLANE_FILTER_OPERATORS
from dataplane.table_server import TableServer, TableNotFoundException, TableNotAuthorizedException, ColumnNotFoundException, build_table_spec
from main import create_app

from tests.table_data_good import names, ages, dates, times, datetimes, booleans, series_for_name

schema = [
    {"name": "name", "type": DATA_PLANE_STRING},
    {"name": "age", "type": DATA_PLANE_NUMBER},
    {"name": "date", "type": DATA_PLANE_DATE},
    {"name": "time", "type": DATA_PLANE_TIME_OF_DAY},
    {"name": "datetime", "type": DATA_PLANE_DATETIME},
    {"name": "boolean", "type": DATA_PLANE_BOOLEAN}
]

rows = [[names[i], ages[i], dates[i], times[i], datetimes[i], booleans[i]] for i in range(len(names))]

table = RowTable(schema, rows)

schema = [{"name": "name", "type": DATA_PLANE_STRING}, {"name": "age", "type": DATA_PLANE_NUMBER}]
rows1 = [['Tim', 24], ['Jill', 23], ['jane', 45], ['Fred', 57]]

rows2 = [['John', 65], ['George', 85], ['Alfred', 76], ['Susie', 77]]

unprotected = RowTable(schema, rows1)
protected = RowTable(schema, rows2)
headers = [{"variable": "test", "value": "foo"}]

def _add_tables():
    add_data_plane_table('table1', table)
    add_data_plane_table('unprotected', unprotected)
    add_data_plane_table('protected', protected, headers)


@pytest.fixture


@pytest.fixture
def client():
    app = create_app()
    app.config['TESTING'] = True

    with app.test_client() as client:
        yield client




def test_add_table():
    with pytest.raises(InvalidDataException) as exception:
        add_data_plane_table('Foo', None)
    with pytest.raises(InvalidDataException) as exception:
        add_data_plane_table('Foo', 1)
    _add_tables()
    assert(table_servers['table1']['table'] == table)
    assert(table_servers['table1']['headers'] == [])
    assert(table_servers['unprotected']['table'] == unprotected)
    assert(table_servers['unprotected']['headers'] == [])
    assert(table_servers['protected']['table'] == protected)
    assert(table_servers['protected']['headers'] == headers)



def test_get_tables(client):
    _add_tables()
    # pytest.set_trace()
    response = client.get('/get_tables')
    assert(set(response.json.keys()) == {'unprotected', 'table1'})
    # pytest.set_trace()
    

