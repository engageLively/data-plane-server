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
from json import JSONDecodeError, loads
import pandas as pd

from flask import Blueprint, abort, jsonify, request


from dataplane.data_plane_utils import DATA_PLANE_NUMBER
from dataplane.data_plane_utils import DATA_PLANE_SCHEMA_TYPES
from dataplane.data_plane_utils import InvalidDataException
from dataplane.data_plane_table import DataPlaneTable, check_valid_spec

data_plane_server_blueprint = Blueprint('data_plane_server', __name__)

table_servers = {}


def _get_all_tables():
    '''
    Get all the tables.  This
    is to support a request for a numeric_spec or all_values for a column name when the
    table_name is not specified. In this case, all tables will be searched for this column name.
    
    Returns:
        a list of all tables which are authorized
    '''
    servers = table_servers.values()
    return [server["table"] for server in servers if _server_authorized(server)]

def _check_headers(headers):
    '''
    Each header should be a dictionary of the form "variable": <string>  "value": <string>, which 
    are the authorization tokens required to access this table.  Doesn't return  a value: raises an error if a header doesn't match
    '''
    for header in headers:
        assert "variable" in header, f'header {header} does not have a variable field'
        assert "value" in header, f'header {header} does not have a value field'

def add_data_plane_table(table_name, table, required_headers = []):
    '''
    Register a DataPlaneTable to serve data for a specific table name.
    Raises an InvalidDataException if table_name is None or data_plane_table is None or is not an instance of DataPlaneTable.

    Arguments:
        table_name: name to register the server for
        data_plane_table: an instance of DataPlaneTable which services the requests
        required_headers: a list of the form {"variable": <string>, "value": <string>}.  The required headers re
        
    '''
    try:
        assert table is not None, "table cannot be None"
        # bad_type = type(table)
        # msg = f'table must be an instance of DataPlaneTable, not {bad_type}'
        # assert isinstance(table, DataPlaneTable), msg
        try:
            msg = 'table must be an instance of DataPlaneTable'
            assert table.is_dataplane_table, msg
        except AttributeError:
            raise InvalidDataException(msg)
        _check_headers(required_headers)
    except AssertionError as assertion_error:
        raise InvalidDataException(assertion_error)
    
    table_servers[table_name] = {
        "table": table,
        "headers": required_headers
    }

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
        server = table_servers[table_name]
        check_authorization(request_api, table_name, server)
        return server["table"]
    except KeyError:
        msg = f'No handler defined for table {table_name} for request {request_api}'
        _log_and_abort(msg)

def _get_table_server(request_api):
    '''
    Internal use.  Get the server for a specific table_name and return it.
    Aborts the request with a 400 if the table isn't found.  Aborts with a 403 if the
    table isn't authorized

    Arguments:
        request_api: api  of the request
    '''
    table_name = request.headers.get('Table-Name')
    return _table_server_if_authorized(request_api, table_name)
    
    


def _get_table_servers(request_api):
    '''
    Internal use.  Get the server for a specific table_name, or all servers if the name is null.
    Aborts the request with a 400 if the table isn't found.

    Arguments:
        request_api: api  of the request
    '''
    table_name = request.headers.get('Table-Name')
    
    if table_name is not None:
        return [_table_server_if_authorized(request_api, table_name)]
    else:
        authorized_servers = _get_all_tables()
        if len(authorized_servers) == 0:
            message = f'No authorized servers found  for request {request_api}'
            _log_and_abort(message)
        else:
            return authorized_servers


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

def _server_authorized(server):
    '''
    Check to see if the server is authorized.  
    The server is authorized iff:
    1. This table doesn't require authentication
    2. The authentication in the header matches the authentication required
    Arguments:
        server -- the table server to check for authentication
    Returns:
        True iff the header meets the authentication requirements
    '''
    if server["headers"] is None:
        return True
    if len(server["headers"]) == 0:
        return True
    for header in server["headers"]:
        auth_value = request.headers.get(header["variable"])
        if auth_value is None or auth_value != header["value"]:
            return False
    return True


def check_authorization(route, table_name, server):
    '''
    Check that the required headers are present for the table.
    If they are not, abort with a 403 (not authorized) and log it
    Arguments:
        route: The route of the request, required for abort message
        table_name: The name of the table of the request, required for abort message
        server: the table server, with the required variables
    '''
    # Message doesn't contain information about variables which are missing -- should it?
    msg = f'Table {table_name} requires authentication, authentication missing.  Request is {route}'
    if not _server_authorized(server):
            _log_and_abort(msg, 403)





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


    server = _get_table_server('get_filtered_rows')
    # Check to make sure that 
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

    servers = _get_table_servers('/get_all_values')
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
        server  = item[1]
        if _server_authorized(server):
            result[item[0]] = server["table"].schema
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
    servers = _get_table_servers('/get_table_spec')
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
