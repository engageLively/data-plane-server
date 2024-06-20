'''
A DataPlaneTable class and associated utilities.  The DataPlaneTable class is initialized
with the table's schema,  single function,get_rows(), which returns the rows of the table.  To
use a  DataPlaneTable instance, instantiate it with the schema and a get_rows() function.
The DataPlaneTable instance can then be passed to a DataPlaneServer with a call to
galyleo_server_framework.add_table_server, and the server will then be able to serve
the tables automatically using the instantiated DataPlaneTable.
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

from functools import reduce
from math import nan, isnan
import re
import pandas as pd
import datetime
import requests
import json

from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_DATETIME, DATA_PLANE_DATE, \
    DATA_PLANE_SCHEMA_TYPES, DATA_PLANE_STRING, DATA_PLANE_TIME_OF_DAY
from dataplane.data_plane_utils import InvalidDataException
from dataplane.data_plane_utils import jsonifiable_column, jsonifiable_row, jsonifiable_rows, jsonifiable_value, type_check
from dataplane.data_plane_utils import convert_list_to_type, convert_dict_to_type, convert_rows_to_type_list
from dataplane.data_plane_filter import DataPlaneFilter

        
def _select_entries_from_row(row, indices):
    # Pick the entries of row trhat are in indices, maintaining the order of the
    # indices.  This is to support the column-choice operation in DataPlaneTable.get_filtered_rows
    # Arguments:
    #     row: the tow of vaslues
    #     indices: the indices to pick
    # Returns:
    #     The subset of the row corresponding to the indices
    return [row[i] for i in range(len(row)) if i in indices]



DEFAULT_HEADER_VARIABLES = {"required": [], "optional": []}
'''
The Default for header variables for a table is both required and optional lists are empty.
'''

def get_errors(entry):
    '''
    A Utility to make sure that a schema entry is valid.  It must have a name, a type, both must be strings, 
    and the type is one of DATA_PLANE_SCHEMA_TYPES.
    Arguments:
        entry: a dictionary with (at least) the keys name, type
    Returns:
        A list of errors, which will be the empty list if no errors are found.
    '''
    if not type(entry) == dict:
        return [f'Schema entry {entry} must be a dictionary, not {type(entry)}']
    result = []
    keys = set(entry.keys())
    if not 'name' in keys:
        result.append(f'Column {entry} must have a name')
    elif type(entry['name']) != str:
        result.append(f'Name of column {entry} must be a string')
    if not 'type' in keys:
        result.append(f'Column {entry} must have a type')
    elif not (type(entry['type']) == str and entry['type'] in DATA_PLANE_SCHEMA_TYPES):
        result.append(f'Type of column {entry} must be one of {DATA_PLANE_SCHEMA_TYPES}' )
    return result
            
    

class DataPlaneTable:
    '''
    A DataPlaneTable.  This is the abstract superclass for all data plane tables, and 
    implements the schema methods of every DataPlane class.  The data methods are implemented
    by the concrete classes.  Any new DataPlaneTable class should:
    1. Subclass DataPlaneTable
    2. Have a constructor with the argument schema
    3. call super(<classname, self).__init__(schema) in the constructor
    4. Implement the methods:
        (a) all_values(self, column_name, jsonify = False)
        (b) range_spec(self, column_name, jsonify = False)
        (c) get_filtered_rows_from_filter(self, filter, columns = None, jsonify = False)
        (d) to_json(self)
    where:
        i. column_name is the name of the column to get the values/range_spec from
        ii. if jsonify = True, return the results as a JSON string, otherwise just as the 
            appropriate structure
            iia. list from all_values
            iib. dict "max_val", "min_val" from range_spec
            iic. list of lists from get_filtered_rows_from_filter)
        iii. filter is a an instance of DataPlaneFilter
        iv. if columns is not None for get_filtered_rows, only return entries from those columns
            in the result from get_filtered_rows
    Arguments:
        schema: a list of records of the form {"name": <column_name, "type": <column_type>}.
           The column_type must be a type from galyleo_constants.DATA_PLANE_TYPES.
    '''
    def __init__(self, schema):
        if type(schema) != list:
            raise InvalidDataException(f'The schema must be a list of dictionaries, not {type(schema)}')
        error_entries = [get_errors(entry) for entry in schema]
        error_entries = [entry for entry in error_entries if len(entry) > 0]
        if len(error_entries) > 0:
            raise InvalidDataException(f"Errors in schema {schema}: {error_entries}")
           
        self.schema = schema
        self.is_dataplane_table = True

    def column_names(self):
        '''
        Return the names of the columns
        '''
        return [column["name"] for column in self.schema]

    def column_types(self):
        '''
        Return the types of the columns
        '''
        return [column["type"] for column in self.schema]

    def get_column_type(self, column_name):
        '''
        Returns the type of column column_name, or None if this table doesn't have a column with
        name  column_name.

        Arguments:
            column_name: name of the column to get the type for
        '''
        matches = [column["type"] for column in self.schema if column["name"] == column_name]
        if len(matches) == 0:
            return None
        else:
            return matches[0]
    
    def all_values(self, column_name: str, jsonify = False):
        '''
        get all the values from column_name
        Arguments:
            column_name: name of the column to get the values for
            jsonify: if true, return the result in a form that can be turned
                into json 

        Returns:
            List of the values, either in json form (if jsonify = True) or in the
            appropriate datatyp (if jsonify = False)

        '''
        raise InvalidDataException(f'all_values has not been in {type(self)}.__name__')
    
    

    def range_spec(self, column_name: str, jsonify = False):
        '''
        Get the dictionary {min_val, max_val} for column_name
        Arguments:

            column_name: name of the column to get the range spec for

        Returns:
            the minimum and  maximum of the column

        '''
        raise InvalidDataException(f'range_spec has not been in {type(self)}.__name__')
    
    def get_filtered_rows_from_filter(self, filter=None, columns=[], jsonify = False):
        '''
        Returns the rows for which the  filter returns True.  Returns as 
        a json list if jsonify is True, as a list of the appropriate types otherwise

        Arguments:
            filter: A DataPlaneFilter 
            columns: the names of the columns to return.  Returns all columns if absent
            jsonify: if True, returns a JSON list
        Returns:
            The subset of self.get_rows() which pass the filter as a JSON list if
            jsonify is True or as a list if jsonify is False
        '''
        raise InvalidDataException(f'get_filtered_rows_from_filter has not been in {type(self)}.__name__')


    def get_filtered_rows(self, filter_spec=None, columns=[], jsonify = False):
        '''
        Filter the rows according to the specification given by filter_spec.
        Returns the rows for which the resulting filter returns True.Returns as 
        a json list if jsonify is True, as a list of the appropriate types otherwise

        Arguments:
            filter_spec: Specification of the filter, as a dictionary
            columns: the names of the columns to return.  Returns all columns if absent
            jsonify: if True, returns a JSON list
        Returns:
            The subset of self.get_rows() which pass the filter as a JSON list if
            jsonify is True or as a list if jsonify is False
        '''
        # Note that we don't check if the column names are all valid
        filter = DataPlaneFilter(filter_spec, self.schema) if filter_spec is not None else None
        return self.get_filtered_rows_from_filter(filter = filter, columns=columns, jsonify=jsonify)
    
    def to_json(self):
        '''
        Return the JSON form of this table, for saving on disk or transmission.
        '''
        raise InvalidDataException(f'to_json has not been in {type(self)}.__name__')

class DataPlaneFixedTable(DataPlaneTable):
    '''
    A DataPlaneFixedTable: This is a convenience class for subclasses which generate a fixed 
    number of rows locally, independent of filtering. This is instantiated with a function get_rows() which  delivers the
    rows, rather than having them explicitly in the Table.  Note that get_rows() *must* return 
    a list of rows, each of which has the appropriate number of entries of the appropriate types.
    all_values, range_spec, and get_filtered_rows_from_filter are all implemented on top of 
    get_rows.  Note that these methods can be overridden in a subclass if there is a
    more efficient method than the obvious implementation, which is what's implemented here.

    Arguments:
        schema: a list of records of the form {"name": <column_name, "type": <column_type>}.
           The column_type must be a type from galyleo_constants.DATA_PLANE_TYPES.
        get_rows: a function which returns a list of list of values.  Each component list
            must have the same length as schema, and the jth element must be of the
            type specified in the jth element of schema
    '''

    def __init__(self, schema, get_rows):
        super(DataPlaneFixedTable, self).__init__(schema)
        self.get_rows = get_rows

    # This is used to get the names of a column from the schema

    
    def all_values(self, column_name: str, jsonify = False):
        '''
        get all the values from column_name
        Arguments:
            column_name: name of the column to get the values for
            jsonify: if true, return the result in a form that can be turned
                into json 

        Returns:
            List of the values, either in json form (if jsonify = True) or in the
            appropriate datatyp (if jsonify = False)

        '''
        try:
            index = self.column_names().index(column_name)
        except ValueError as original_error:
            raise InvalidDataException(f'{column_name} is not a column of this table') from original_error
        data_plane_type = self.schema[index]["type"]
        rows = self.get_rows()
        result = list(set([row[index] for row in rows]))
        result.sort()
        return jsonifiable_column(result, data_plane_type) if jsonify else result
    
    def check_column_type(self, column_name):
        '''
        For testing.  Makes sure that all the entries in column_name are the right type
        No return, but throws an InvalidDataException if there's a bad element in the column
        '''
        value_list = self.all_values(column_name, False)
        required_type = self.get_column_type(column_name)
        bad_values = [val for val in value_list if not type_check(required_type, val)]
        if len(bad_values) > 0:
            raise InvalidDataException(f'Values {bad_values} could not be converted to {required_type} in column {column_name}')
        

    def range_spec(self, column_name: str, jsonify = False):
        '''
        Get the dictionary {min_val, max_val} for column_name
        Arguments:

            column_name: name of the column to get the range spec for

        Returns:
            the minimum and  maximum of the column

        '''
        entry = [column for column in self.schema if column["name"] == column_name]
        if len(entry) == 0:
            raise InvalidDataException(f'{column_name} is not a column of this table')

        values = self.all_values(column_name, jsonify) 

        return {"max_val": values[-1], "min_val": values[0]}
    
    def get_filtered_rows_from_filter(self, filter=None, columns=[], jsonify = False):
        '''
        Returns the rows for which the  filter returns True.  Returns as 
        a json list if jsonify is True, as a list of the appropriate types otherwise

        Arguments:
            filter: A DataPlaneFilter 
            columns: the names of the columns to return.  Returns all columns if absent
            jsonify: if True, returns a JSON list
        Returns:
            The subset of self.get_rows() which pass the filter as a JSON list if
            jsonify is True or as a list if jsonify is False
        '''
         # Note that we don't check if the column names are all valid
        if columns is None: columns = []  # Make sure there's a value
        if filter is None:
            rows = self.get_rows()
        else:
            rows = filter.filter(self.get_rows())
        if columns == []:
            result =  rows
            column_types = self.column_types()
        else:
            names = self.column_names()
            column_indices = [i for i in range(len(names)) if names[i] in columns]
            all_types = self.column_types()
            column_types = [all_types[i] for i in column_indices]
            result = [[row[i] for i in column_indices] for row in rows]
        return jsonifiable_rows(result, column_types) if jsonify else result


    def to_dataframe(self):
        '''
        Convert the table to a PANDAS DataFrame.  This is very straightforward; just 
        use get_rows to get the rows and convert the schema to the appropriate dtypes.
        Note this relies on PANDAS type inference.
        '''
        
        return  pd.DataFrame(self.get_rows(), columns = self.column_names())
    
    def to_json(self):
        '''
        Return the JSON form of this table, for saving on disk or transmission.
        '''
        # Since the columns are already a dictionary, they are simply directly jsonified.  For the rows,
        # just use the jsonify methods from data_plane_utils

        return json.dumps({
            "type": "DataPlaneTable",
            "columns": self.schema,
            "rows": jsonifiable_rows(self.get_rows(), self.column_types())
        })

    

class DataFrameTable(DataPlaneFixedTable):
    '''
    A simple utility class to serve data from a PANDAS DataFrame.  The general idea is 
    that the values are in the PANDAS Dataframe, which must have the same column names
    as the schema and compatible types.
    '''
    def __init__(self, schema, dataframe):
        super(DataFrameTable, self).__init__(schema, self._get_rows)
        self.dataframe = dataframe.copy()
        # Make sure the column names and types match
        self.dataframe.columns = self.column_names()
         # make sure that the types match
        for column in schema:
            column_values = self.dataframe[column["name"]].tolist()
            try:
                fixed_series = convert_list_to_type(column["type"], column_values)
                self.dataframe[column["name"]] = fixed_series
            except Exception as exc:
                raise InvalidDataException(f'error {exc} converting {column["name"]}')

    def _get_rows(self):
        '''
        Very simple: just return the rows
        '''
        return self.dataframe.values.tolist()

    def to_dataframe(self):
        return self.dataframe.copy()


class RowTable(DataPlaneFixedTable):
    '''
    A simple utility class to serve data from a static list of rows, which
    can be constructed from a CSV file, Excel File, etc.  The idea is to
    make it easy for users to create and upload simple datasets to be
    served from a general-purpose server.  Note that this will need some
    authentication.
    '''

    def __init__(self, schema, rows):
        super(RowTable, self).__init__(schema, self._get_rows)
        type_list = self.column_types()
        self.rows = convert_rows_to_type_list(type_list, rows)
    
    def _get_rows(self):
        return [row for row in self.rows]
    
        

class  RemoteCSVTable(DataPlaneFixedTable):
    '''
    A very common format for data interchange on the Internet is a downloadable
    CSV file.  It's so common it's worth making a class, just for this.  The
    idea is that, when a get_rows request comes in, we download the table
    into a dataframe and then return the list of rows, perhaps after doing
    unit conversion.  We use an internal DataFrame table for this, so that the 
    logic of converting to/from dataframes is centralized
    '''

    def __init__(self, schema, url):
        super(RemoteCSVTable, self).__init__(schema, self.get_rows)
        self.url = url
        self.reset_dataframe()

    def reset_dataframe(self):
        self.dataframe_table = DataFrameTable(self.schema, pd.read_csv(self.url))

    def get_rows(self):
        return self.dataframe_table.get_rows()

    def to_dataframe(self):
        return self.dataframe_table.to_dataframe()  

    def to_json(self):
        return {
            "type":  "RemoteCSVTable",
            "columns": self.schema,
            "connector": {
                "url": self.url,
                "type": "csv"
            }
        }      


def _column_names(schema):
    return [entry["name"] for entry in schema]
        
class RemoteDataPlaneTable(DataPlaneTable):
    '''
    A DataPlane Table on a remote server.  This just has a schema, an URL, and 
    header variables. This is the primary class for the client side of the DataPlane,
    and in many packages would be a separate client module.  However, the DataPlane is 
    designed so that Remote Tables can be used to serve local tables, so this 
    is part of a server-side framework to.
    Parameters:
        table_name: name of the resmote stable
        schema: schema of the remote table
        url: url of the server hosting the remore table
        header_dict: dictionary of variables and values required to access the table
    Throws:
        InvalidDataException if the table doesn't exist on the server, the 
        url is unreachable, the schema doesn't match the downloaded schema

    ''' 
    def __init__(self, table_name, schema, url, header_dict = None): 
        super(RemoteDataPlaneTable, self).__init__(schema)
        self.url = url
        self.schema = schema
        self.table_name = table_name
        self.header_dict = header_dict
        self.ok = False

    def _connect_error(self, msg):
        self.ok = False
        raise InvalidDataException(f'Error: {msg}')

    def _check_entry_match(self, schema, index, field, mismatches):
        if self.schema[index][field] == schema[index][field]: return
        mismatches.append(f'Mismatch in field {field} at entry {index}. Server value: {schema[index][field]}, declared value: {self.schema[index][field]}')

    def _check_schema_match(self, schema):
        if len(schema) != len(self.schema):
            self._connect_error(f'Server schema {_column_names(schema)} has {len(schema)} columns, declared schema {_column_names(self.schema)} has {len(self.schema)} columns')
        mismatches = []
        for i in range(len(schema)):
            self._check_entry_match(schema, i, "name", mismatches)
            self._check_entry_match(schema, i, "type", mismatches)
        
        if len(mismatches) > 0:
            mismatch_report = 'Schema mismatch: ' + ', '.join(mismatches)
            self._connect_error(mismatch_report)
    
    def connect_with_server(self):
        '''
        Connect with the server, ensuring that the server is:
        a. a DataPlane server
        b. has self.table_name in its list of tables
        c. the table there has a matching schema
        '''
        
        try:
            response = requests.get(f'{self.url}/get_tables')
            if response.status_code >= 300:
                self._connect_error(f'Bad connection with {self.url}: code {response.status_code}')
        except Exception as e:
            self._connect_error(f'Error connecting with {self.url}/get_tables: {repr(e)}')
        try:
            server_tables = response.json()
        except Exception as e:
            self._connect_error(f'Error {repr(e)} reading tables from  {self.url}/get_tables')
        if self.table_name in server_tables:
            server_schema = server_tables[self.table_name]
            self._check_schema_match(server_schema)
        else:
            self._connect_error(f'Server at {self.url} does not have table {self.table_name}')
        # if we get here, everything worked:
        self.ok = True
        
        # also check to make sure that we can authenticate to the table.  See /get_table_spec

    
        
    def _check_column_and_get_type(self, column_name):
         if not column_name in self.column_names():
            raise InvalidDataException(f'Column {column_name} is not a column of {self.table_name}')
         return self.get_column_type(column_name)

        
    def _do_request(self,  request):
        # check to see if we have a connection, and then do a GET request using GET,
        # supplying header variables if required. 
        # Note that the wire format is json, so the return from this function is json,
        # and (if required) converted to the right datatype by the calling routing
        if not self.ok:
            self.connect_with_server()
        try:
            response = requests.get(request)
            if response.status_code >= 300:
                raise InvalidDataException(f'{request} returned error code{response.status_code}')
            return response.json()
        except Exception as exc:
            raise InvalidDataException(f'Exception {repr(exc)} ocurred in {request}')
        
    def all_values(self, column_name: str, jsonify_results = False):
        '''
        get all the values from column_name
        Arguments:

            column_name: name of the column to get the values for

        Returns:
            List of the values, in the appropriate type if jsonify_results is False,
            otherwise in the appropriate json format

        '''
        # use _do_request to execute the request, then, if jsonify_results 
        # parameters are ['table_name', 'column_name']
        column_type = self._check_column_and_get_type(column_name)
        request = f'{self.url}/get_all_values?table_name={self.table_name}&column_name={column_name}'
        result = self._do_request(request)
        if jsonify_results:
            return result
        else:
            return convert_list_to_type(column_type, result)

        

    def range_spec(self, column_name: str, jsonify_results = False):
        '''
        Get the dictionary {min_val, max_val} for column_name
        Arguments:

            column_name: name of the column to get the range spec for

        Returns:
            the minimum and  maximum of the column

        '''
        column_type = self._check_column_and_get_type(column_name)
        request = f'{self.url}/get_range_spec?table_name={self.table_name}&column_name={column_name}'
        result = self._do_request(request)
        if jsonify_results:
            return result
        else:
            return convert_dict_to_type(column_type, result)
        

    def get_filtered_rows_from_filter(self, filter=None, columns=[], jsonify = False):
        '''
        Returns the rows for which the  filter returns True.  Returns as 
        a json list if jsonify is True, as a list of the appropriate types otherwise

        Arguments:
            filter: A DataPlaneFilter 
            columns: the names of the columns to return.  Returns all columns if absent
            jsonify: if True, returns a JSON list
        Returns:
            The subset of self.get_rows() which pass the filter as a JSON list if
            jsonify is True or as a list if jsonify is False
        '''
        if filter is None:
            return self.get_filtered_rows(columns=columns, jsonify = jsonify)
        else:
            return self.get_filtered_rows(filter.to_filter_spec(), columns=columns, jsonify = jsonify)
    
    def get_filtered_rows(self, filter_spec=None, columns=[], jsonify = False):
        '''
        Filter the rows according to the specification given by filter_spec.
        Returns the rows for which the resulting filter returns True.

        Arguments:
            filter_spec: Specification of the filter, as a dictionary
            columns: the names of the columns to return.  Returns all columns if absent
        Returns:
            The subset of self.get_rows() which pass the filter
        '''
        if not self.ok:
            self.connect_with_server()
        request = f'{self.url}/get_filtered_rows'
        data = {
            'table': self.table_name
        }
        if filter_spec:
            data['filter'] = filter_spec
        if columns is not None and len(columns) > 0:
            data['columns'] = columns
            data_plane_type_list = [column["type"] for column in self.schema if column["name"] in columns]
        else: 
            data_plane_type_list = self.column_types() 
        try:
            response = requests.post(request, json=data, headers=self.header_dict) if self.header_dict is not None else requests.post(request, json=data)
            if response.status_code >= 300:
                raise InvalidDataException(f'get_filtered_rows to {self.url}: caused error response {response.status_code}')
            result = response.json() 
        except Exception as exc:
            raise InvalidDataException(f'Error in get_filtered_rows to {self.url}: {repr(exc)}')
        if jsonify:
            return result
        else:
            return convert_rows_to_type_list(data_plane_type_list, result)
        
                    
