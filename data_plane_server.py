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


import logging
import csv
import datetime
from json import JSONDecodeError, loads

from flask import Blueprint, abort, jsonify, request

from data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_DATE, DATA_PLANE_DATETIME, DATA_PLANE_NUMBER, DATA_PLANE_STRING, DATA_PLANE_TIME_OF_DAY
from data_plane_utils import DATA_PLANE_SCHEMA_TYPES
from data_plane_utils import InvalidDataException
from data_plane_table import DataPlaneTable, RowTable, check_valid_spec


data_plane_server_blueprint = Blueprint('data_plane_server', __name__)

table_servers = {}


def _convert_type(type, value):
    if type == DATA_PLANE_NUMBER:
        return float(value)
    if type == DATA_PLANE_DATE:
        # return a parsed datetime
        return datetime.datetime.fromisoformat(value)
    if type == DATA_PLANE_DATETIME:
        # return a parsed datetime
        return datetime.datetime.fromisoformat(value)
    if type == DATA_PLANE_TIME_OF_DAY:
        # return a parsed datetime
        return datetime.datetime.fromisoformat(value)
    if type == DATA_PLANE_BOOLEAN:
        return True if value else False
    return value

def _convert_row(types, row):
    return [_convert_type(types[i], row[i]) for i in range(len(types))]
    

def create_server_from_csv(table_name, path_to_csv_file):
    '''
    Create a server from a CSV file.The file must meet the format for a RowTable:
    1. Each row must contain the same number of columns;
    2. The first row (row 0) are the names of the columns
    3. The second row (row 1)  has the types of the columns
    4. The type of each entry in rows 2-n must match the declared type of the column
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
    try:
        final_rows = [_convert_row(rows[1], row) for row in rows[2:]]
        server = RowTable(schema, final_rows)
        add_data_plane_table(table_name, server)

    except ValueError as error:
        raise InvalidDataException(f'{error} raised during type conversion')


def _get_all_tables():
    '''
    Get all the tables.  This
    is to support a request for a numeric_spec or all_values for a column name when the
    table_name is not specified. In this case, all tables will be searched for this column name.
    
    Returns:
        a list of all tables 
    '''
    return table_servers.values()

def add_data_plane_table(table_name, data_plane_table):
    '''
    Register a DataPlaneTable to serve data for a specific table name.
    Raises an InvalidDataException if table_name is None or data_plane_tabe is None or is not an instance of DataPlaneTable.

    Arguments:
        table_name: name to register the server for
        data_plane_table: an instance of DataPlaneTable which services the requests
        
    '''
    try:
        assert data_plane_table is not None, "data_plane_table cannot be None"
        bad_type = type(data_plane_table)
        msg = f'data_plane_table must be an instance of DataPlaneTable, not {bad_type}'
        assert isinstance(data_plane_table, DataPlaneTable), msg
    except AssertionError as assertion_error:
        raise InvalidDataException from assertion_error
    table_servers[table_name] = data_plane_table

def _log_and_abort(message):
    '''
    Sent an abort with code 400 and log the error message.  Utility, internal use only

    Arguments:
        message: string with the message to be logged/sent
    '''
    logging.error(message)
    abort(400, message)

def _get_table_server(request_api):
    '''
    Internal use.  Get the server for a specific table_name and return it.
    Aborts the request with a 400 if the table isn't found.

    Arguments:
        request_api: api  of the request
    '''
    table_name = request.headers.get('Table-Name')
    try:
        return table_servers[table_name]
    except KeyError:
        msg = f'No handler defined for table {table_name} for request {request_api}'
        _log_and_abort(msg)
    


def _get_table_servers(request_api):
    '''
    Internal use.  Get the server for a specific table_name, or all servers if the name is null.
    Aborts the request with a 400 if the table isn't found.

    Arguments:
        request_api: api  of the request
    '''
    table_name = request.headers.get('Table-Name')
    
    if table_name is not None:
        try:
            return [table_servers[table_name]]
        except KeyError:
            msg = f'No handler defined for table {table_name} for request {request_api}'
            _log_and_abort(msg)
    else:
        tables = _get_all_tables()
        if len(tables) == 0:
            message = f'No tables found  for request {request_api}'
            _log_and_abort(message)
        else:
            return tables


def _check_required_parameters(handle, parameter_set):
    '''
    Check to make sure the required parameters are in the parameter set
    required for a request, aborting if they aren't. This can only be used
    with get requests, since it pulls this from the args multidict.
    This is designed for internal use only

    Arguments:
        handle: the URL handle, for error reporting
        parameter_set: the set of parameters required
    '''
    sent_parameters = set(request.args.keys())
    missing_parameters = parameter_set - sent_parameters
    if len(missing_parameters) > 0:
        _log_and_abort(f'Missing arguments to {handle}: {missing_parameters}')


@data_plane_server_blueprint.route('/hello')
def hello():
    '''
    Just a simple get target to make sure that the framework is working

    Arguments:
       None
    '''
    return "hello"

@data_plane_server_blueprint.route('/echo_headers', methods = ['POST', 'GET'])
def echo_headers():
    '''
    Echo the headers back, for debugging

    Arguments:
        None
    '''
    result = {}
    for key in request.headers.keys():
        result[key] = request.headers[key]
    return jsonify(result)

# @data_plane_server_blueprint.route('/echo_post', methods=['POST'])
# def echo_post():
#     '''
#     Echo the request
#     '''
#     return jsonify(request.json)


@data_plane_server_blueprint.route('/get_filtered_rows', methods=['GET'])
def get_filtered_rows():
    '''
    Get the filtered rows from a request.  In the initializer, this
    was registered for the /get_filtered_rows route.  Gets the filter_spec
    from the Filter-Spec header variable If there is no filter_spec, returns
    all rows using server.get_rows().  Aborts with a 400 if there is no
    table_name, or if check_valid_spec or get_filtered_rows throws an
    InvalidDataException, or if the filter_spec is not valid JSON.

    Arguments:
        None
    Returns:
        The filtered rows as a JSONified list of lists
    '''
    filter_spec = None
    filter_spec_as_json = request.headers.get('Filter-Spec')
    if filter_spec_as_json is not None:
        try:
            filter_spec = loads(filter_spec_as_json)
        except JSONDecodeError as error:
            _log_and_abort(f'Bad Filter Specification: {filter_spec_as_json}.  Error {error.msg}')


    server = _get_table_server('get_filtered_rows')
    if filter_spec is not None:
        try:
            check_valid_spec(filter_spec)
            return jsonify(server.get_filtered_rows(filter_spec))
        except InvalidDataException as invalid_error:
            _log_and_abort(invalid_error)
    else:
        return jsonify(server.get_rows())

def _is_numeric_column(table_server, column_name):
    '''
    Internal use only.  Returns True iff the table_server has a column with name column_name,
     and if the type is DATA_PLANE_NUMBER

    Arguments:
        table_server: the table server to check
        column_name: the name of the column_name
    '''
    column_type = table_server.get_column_type(column_name)
    return column_type is not None and column_type == DATA_PLANE_NUMBER


@data_plane_server_blueprint.route('/get_numeric_spec')
def get_numeric_spec():
    '''
    Target for the /get_numeric_spec route.  Makes sure that column_name is specified
    in the call, and that if table_name is present, it is registered, then returns the
    numeric spec {"min_val", "max_val", "increment"} as a JSONified dictionary.  Uses
    server.get_numeric_spec(column_name) to create the numeric spec.  Aborts with a 400
    for missing arguments, bad table name, or if there is no column_name in the arguments.

    Arrguments:
            None
    '''

    servers = _get_table_servers('/get_numeric_spec')
    column_name = request.args.get('column_name')
    if column_name is not None:
        matching_servers = [server for server in servers if _is_numeric_column(server, column_name)]
        if len(matching_servers) == 0:
            _log_and_abort(f'/get_numeric_spec found no numeric columns of name {column_name}')
        spec = matching_servers[0].numeric_spec(column_name)
        for server in matching_servers[1:]:
            serv_spec = server.numeric_spec(column_name)
            spec["max_val"] = max(spec["max_val"], serv_spec["max_val"])
            spec["min_val"] = min(spec["min_val"], serv_spec["min_val"])
            spec["increment"] = min(spec["increment"], serv_spec["increment"])
        return jsonify(spec)
    else:
        _log_and_abort('/get_numeric_spec requires a parameter "column_name"')

@data_plane_server_blueprint.route('/get_all_values')
def get_all_values():
    '''
    Target for the /get_all_values route.  Makes sure that column_name is specified in the call,
    and that if table_name is present, it is registered, then returns the distinct values as a
    JSONified list.  Uses server.get_all_values(column_name) to get the values.  Aborts with a
    400 for missing arguments, bad table name, or if there is no column_name in the arguments.

    Arguments:
        None
    '''

    servers = _get_table_servers('/get_numeric_spec')
    column_name = request.args.get('column_name')
    if column_name is not None:
        try:
            matching_servers = [server for server in servers if server.get_column_type(column_name) is not None]
            if len(matching_servers) == 0:
                _log_and_abort(f'/get_all_values found no  columns of name {column_name}')
            values_set = set(matching_servers[0].all_values(column_name))
            for server in matching_servers[1:]:
                values_set = values_set.union(set(server.all_values(column_name)))
            result = list(values_set)
            result.sort()
            return jsonify(result)
        except InvalidDataException as error:
            _log_and_abort(f'Error in get_all_values for column {column_name}: {error}')
    else:
        _log_and_abort('/get_all_values requires a parameter "column_name"')

@data_plane_server_blueprint.route('/get_tables')
def get_tables():
    '''
    Target for the /get_tables route.  Dumps a JSONIfied dictionary of the form:
    {table_name: <table_schema>}, where <table_schema> is a dictionary
    {"name": name, "type": type}

    Arguments:
            None
    '''
    result = {}
    items = table_servers.items()
    for item in items:
        result[item[0]] = item[1].schema
    return jsonify(result)

@data_plane_server_blueprint.route('/get_table_spec')
def get_table_spec():
    '''
    Target for the /get_table_spec route.  Dumps a table_spec, which is a dictionary of the form:
        {
            "header_variables": {"required" : <list of names>, "optional": <list of names>},
            "schema": list of {"name": <string>, "type": <one of DATA_PLANE_TYPEs>}
            
        }

    Arguments:
        None

    '''
    servers = _get_table_servers('/get_numeric_spec')
    return jsonify({"header_variables": servers[0].header_variables, "schema": servers[0].schema})

@data_plane_server_blueprint.route('/help', methods=['POST', 'GET'])
@data_plane_server_blueprint.route('/', methods=['POST', 'GET'])
def show_routes():
    '''
    Show the API for the table server
    Arguments: None
    '''
    pages = [
            {"url": "/, /help", "headers": "", "method": "GET", "description": "print this message"},
            {"url": "/get_tables", "method": "GET", "headers": "", "description": 'Dumps a JSONIfied dictionary of the form:{table_name: <table_schema>}, where <table_schema> is a dictionary{"name": name, "type": type}'},
            {"url": "/get_filtered_rows", "method": "GET", "headers": "Filter-Spec <i>Type Filter Spec, required</i>, Table-Name <i>string, required</i>, Dashboard-Name <i>string, optional</i>", "description": "Get the rows from table Table-Name (and, optionally, Dashboard-Name) which match filter Filter-Spec"},
            {"url": "/get_numeric_spec?column_name<i>string, required</i>", "method": "GET", "headers": "Table-Name <i>string, optional</i>, Dashboard-Name <i>string, optional</i>", "description": "Get the  minimum, maximum, and increment values for column <i>column_name</i>, returned as a dictionary {min_val, max_val, increment}.  If Table-Name and/or Dashboard-Name is specified, restrict to that Table/Dashboard"},
            {"url": "/get_all_values?column_name<i>string, required</i>", "method": "GET", "headers": "Table-Name <i>string, optional</i>, Dashboard-Name <i>string, optional</i>", "description": "Get all the distinct values for column <i>column_name</i>, returned as a sorted list.  If Table-Name and/or Dashboard-Name is specified, restrict to that Table/Dashboard"},
            {"url": "/start", "method": "GET", "description": "ensure that all feeds are being updated"},

        ]
    page_strings = [f'<li>{page}</li>' for page in pages]

    return f'<ul>{"".join(page_strings)}</ul>'
