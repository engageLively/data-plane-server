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
import os
from json import JSONDecodeError, loads
from glob import glob

import pandas as pd

from flask import Blueprint, abort, jsonify, request


from dataplane.data_plane_utils import DATA_PLANE_NUMBER

from dataplane.data_plane_utils import InvalidDataException
from dataplane.data_plane_table import  check_valid_spec
from dataplane.table_server import TableServer, TableNotFoundException, TableNotAuthorizedException, ColumnNotFoundException, build_table_spec

data_plane_server_blueprint = Blueprint('data_plane_server', __name__)

table_server = TableServer()



def _log_and_abort(message, code = 400):
    '''
    Sent an abort with error code (defaut 400) and log the error message.  Utility, internal use only

    Arguments:
        message: string with the message to be logged/sent
    '''
    logging.error(message)
    abort(code, message)

def _table_server_if_authorized(request_api, table_name):
    '''
    Utility for _get_table_server and _get_table_servers.  Get the server for  table_name and return it.
    Aborts the request with a 400 if the table isn't found.  Aborts with a 403 if the
    table isn't authorized

    Arguments:
        request_api: api  of the request
        table_name: the table to get
    '''
    try:
        return table_server.get_table(table_name, request.headers)
    except TableNotFoundException:
        msg = f'No handler defined for table {table_name} for request {request_api}'
        code = 400
    except TableNotAuthorizedException:
        msg = msg = f'Table {table_name} requires authentication, authentication missing.  Request is {request_api}'
        code = 403
    _log_and_abort(msg, code)

        

def _get_table(request_api):
    '''
    Internal use.  Get the server for a specific table_name and return it.
    Aborts the request with a 400 if the table isn't found.  Aborts with a 403 if the
    table isn't authorized

    Arguments:
        request_api: api  of the request
    '''
    table_name = request.headers.get('Table-Name')
    return _table_server_if_authorized(request_api, table_name)




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
    from the Filter-Spec field in the header.  If there is no filter_spec, returns
    all rows using server.get_rows().  Aborts with a 400 if there is no
    table_name, or if check_valid_spec or get_filtered_rows throws an
    InvalidDataException, or if the filter_spec is not valid JSON.

    Arguments:
        None
    Returns:
        The filtered rows as a JSONified list of lists
    '''
    filter_spec = None
    filter_spec_as_json  = request.headers.get('Filter-Spec')
    if filter_spec_as_json is not None:
        try:
            filter_spec = loads(filter_spec_as_json)
        except JSONDecodeError as error:
            _log_and_abort(f'Bad Filter Specification: {filter_spec_as_json}.  Error {error.msg}')


    table = _get_table('get_filtered_rows')
    # If there is no filter, just return the table's rows.  If
    # there is a filter, make sure it's valid and then return the filtered
    # rows
    if filter_spec is not None:
        try:
            check_valid_spec(filter_spec)
            return jsonify(table.get_filtered_rows(filter_spec))
        except InvalidDataException as invalid_error:
            _log_and_abort(invalid_error)
    else:
        return jsonify(table.get_rows())


def _check_required_parameters(route, required_parameters):
    '''
    Internal use only.  Check that the required parameters are present.
    If they aren't, aborts with a 400 and an error message
    Arguments:
        route: the route of the call, for an error message
        required_parameters: the parameters that are supposed to be present
    '''
    missing = [parameter for parameter in required_parameters if request.args.get(parameter) is None]
    if len(missing) > 0:
        parameter_string = f'parameters {set(missing)}' if len(missing) > 1 else f'parameter {missing[0]}'
        msg = 'Missing ' + parameter_string + f'for route {route}'
        _log_and_abort(msg, 400)


@data_plane_server_blueprint.route('/get_range_spec')
def get_range_spec():
    '''
    Target for the /get_range_spec route.  Makes sure that column_name and table_name are  specified in the call, then returns the
    range  spec {"min_val", "max_val","} as a JSONified dictionary. Aborts with a 400
    for missing arguments, missing table, bad column name or if there is no column_name in the arguments, and a 403 if the table is not authorized.

    Arrguments:
            None
    '''
    _check_required_parameters('/get_range_spec', ['table_name', 'column_name'])
    column_name =  request.args.get('column_name')
    table_name = request.args.get('table_name')
    try:
        return table_server.get_range_spec(table_name, column_name, request.headers)
    except TableNotAuthorizedException:
        _log_and_abort(f'Access to table {table_name} not authorized, request /get_range_spec', 403)
    except TableNotFoundException:
        _log_and_abort(f'No  table {table_name} present, request /get_range_spec', 400)
    except ColumnNotFoundException: 
        _log_and_abort(f'No column {column_name} in table {table_name}, request /get_range_spec', 400)


@data_plane_server_blueprint.route('/get_all_values')
def get_all_values():
    '''
    Target for the /get_all_values route.  Makes sure that column_name and table_name are  specified in the call, then returns the
    sorted list of all distinct values in the column.    Aborts with a 400
    for missing arguments, missing table, bad column name or if there is no column_name in the arguments, and a 403 if the table is not authorized.

    Arrguments:
            None
    '''
    _check_required_parameters('/get_all_values', ['table_name', 'column_name'])
    column_name =  request.args.get('column_name')
    table_name = request.args.get('table_name')
    try:
        return table_server.get_all_values(table_name, column_name, request.headers)
    except TableNotAuthorizedException:
        _log_and_abort(f'Access to table {table_name} not authorized, request /get_all_values', 403)
    except TableNotFoundException:
        _log_and_abort(f'No  table {table_name} present, request /get_all_values', 400)
    except ColumnNotFoundException: 
        _log_and_abort(f'No column {column_name} in table {table_name}, request /get_all_values', 400)

@data_plane_server_blueprint.route('/get_tables')
def get_tables():
    '''
    Target for the /get_tables route.  Dumps a JSONIfied dictionary of the form:
    {table_name: <table_schema>}, where <table_schema> is a dictionary
    {"name": name, "type": type}

    Arguments:
            None
    '''
    items = table_server.get_table_dictionary(request.headers)

    return jsonify(items)

@data_plane_server_blueprint.route('/get_table_spec')
def get_table_spec():
    '''
    Target for the /get_table_spec route.  Returns a dictionary of the
    form {table_name: list of required authorization variables}

    Returns:
         A dictionary of the form {table_name: list of required authorization variables}

    '''
    return jsonify(table_server.get_auth_spec())

@data_plane_server_blueprint.route('/init', methods = ['POST', 'GET'])
def init():
    table_server.__init__()
    if os.path.exists('data_plane/tables'):
        files = glob('./data_plane/tables/*.json')
        for filename in files:
            table_server.add_data_plane_table(build_table_spec(filename))
    return jsonify(table_server.get_auth_spec())
        


@data_plane_server_blueprint.route('/help', methods=['POST', 'GET'])
@data_plane_server_blueprint.route('/', methods=['POST', 'GET'])
def show_routes():
    '''
    Show the API for the table server
    Arguments: None
    '''
    pages = [
            {"url": "/, /help", "headers": "", "method": "GET", "description": "print this message"},
            {"url": "/get_tables", "method": "GET", "headers": "<i>as required for authentication</i>", "description": 'Dumps a JSONIfied dictionary of the form:{table_name: <table_schema>}, where <table_schema> is a dictionary{"name": name, "type": type}'},
            {"url": "/get_filtered_rows?&table_name<i>string, required</i>", "method": "GET", "headers": "Filter-Spec <i>Type Filter Spec, required</i>, <i>others as required for authentication</i>", "description": "Get the rows from table Table-Name (and, optionally, Dashboard-Name) which match filter Filter-Spec"},
            {"url": "/get_range_spec?column_name<i>string, required</i>&table_name<i>string, required</i>", "method": "GET", "headers":"<i>as required for authentication</i>", "description": "Get the  minimum, and maximumvalues for column <i>column_name</i> in table<i>table_name</i>, returned as a dictionary {min_val, max_val}."},
            {"url": "/get_all_values?column_name<i>string, required</i>&table_name<i>string, required</i>", "method": "GET", "headers": "<i>as required for authentication</i>", "description": "Get all the distinct values for column <i>column_name</i> in table <i>table_name</i>, returned as a sorted list.  Authentication variables shjould be in headers."},
            {"url": "/get_table_spec", "method": "GET", "description": "Return the dictionary of table names and authorization variables"},
            {"url": "/init", "method": "GET", "description": "Restart the table server and load any initial tables.  Returns the list returned by /get_table_spec"},

        ]
    page_strings = [f'<li>{page}</li>' for page in pages]

    return f'<ul>{"".join(page_strings)}</ul>'
