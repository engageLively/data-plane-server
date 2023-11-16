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


import datetime
from math import nan

from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_DATE, DATA_PLANE_DATETIME, DATA_PLANE_NUMBER, \
    DATA_PLANE_STRING, DATA_PLANE_TIME_OF_DAY

'''
A set of utilities to do type conversion.  CSV files (a primary data source) often
contain strings as numbers, dates, and so forth, and these must be converted to the right
data type on load.
'''


def _convert_to_string(s):
    # handle lists, objects, etc
    return s if isinstance(s, str) else str(s)


def _convert_to_number(x, default_value=nan):
    # Convert x to a number, or to the default value  if connversion fails.  The default_value
    # is a parameter passed in, default to nan
    # Rules:
    #     1. If it's already a number, just return the number
    #     2. Try to convert to a float  first.
    #     3. If float  conversion fails, attempt to convert to an int
    #     4. If everything fails, just return the default
    # the problem with converting to an int first is that int(2.3) == 2
    # which is not what we want to return
    if default_value is None: default_value = nan
    if isinstance(x, int) or isinstance(x, float):
        return x
    try:
        return float(x)
    except ValueError:
        pass
    try:
        return int(x)
    except ValueError:
        return default_value


def _convert_to_boolean(aBool, default_value=False):
    # convert aBool to a boolean, or default_value if not provided.  The rules are simple:
    # 1. if aBool is a boolean, return it
    # 2. if aBool is a string, treturn True iff aBool is in {"True", "true", "1", "t"}
    # 3. if aBool is a number, return True iff aBool != 0
    # 4. Otherwise, return the default value
    if default_value is None: default_value = False
    if type(aBool) == bool: return aBool
    if type(aBool) == str:
        return aBool in {"True", "true", "t"}
    if (isinstance(aBool, int) or isinstance(aBool, float)):
        return aBool != 0
    return default_value


def _convert_to_time(t, format_string=None, default_value=datetime.time(0, 0, 0)):
    # Convert t to a time, or to the default value  if connversion fails.  The default_value
    # is a parameter passed in, default to (0, 0, 0) (12:00:00 AM)
    # The format_string, if passed, governs conversion from a string.  If there is no format_string,
    # strings are parsed in ISO format.
    # The format string is in the form used by datetime.strptime()
    # Rules:
    #     1. If it's already a time, just return the time
    #     2. If it's a date, return the default value
    #     3. If it's a datetime, return the time function
    #     4a. If it's a string and the format_string is provided, use strptime() to parse the string into a time
    #     4b. If it's a string and the format_string is not provided, use isoformat() to parse the string into a time
    #     4c. If it's a string and parsing fails, return  default_value
    #     5. If all else fails, return the default value
    if default_value is None: default_value = datetime.time(0, 0, 0)
    if isinstance(t, datetime.time):
        return t
    if isinstance(t, datetime.date):
        return default_value
    if isinstance(t, datetime.datetime):
        return t.time()
    if isinstance(t, str):
        if format_string:
            try:
                return datetime.datetime.strptime(t, format_string).time()
            except ValueError:
                return default_value
        try:
            return datetime.time.fromisoformat(t)
        except ValueError:
            return default_value
    return default_value


def _convert_to_date(d, format_string=None, default_value=datetime.date(1900, 1, 1)):
    # Convert d to a date, or to the default value  if connversion fails.  The default_value
    # is a parameter passed in, default to (1900,1,1) (January 1, 1900)
    # The format_string, if passed, governs conversion from a string.  If there is no format_string,
    # strings are parsed in ISO format.
    # The format string is in the form used by datetime.strptime()
    # Rules:
    #     1. If it's already a date, just return the date
    #     2. If it's a time, return the default value
    #     3. If it's a datetime, return the date function
    #     4a. If it's a string and the format_string is provided, use strptime() to parse the string into a date
    #     4b. If it's a string and the format_string is not provided, use isoformat() to parse the string into a date
    #     4c. If it's a string and parsing fails, return  default_value
    #     5. If all else fails, return the default value
    if default_value is None: default_value = datetime.date(1900, 1, 1)
    if isinstance(d, datetime.date):
        return d
    if isinstance(d, datetime.time):
        return default_value
    if isinstance(d, datetime.datetime):
        return d.date()
    if isinstance(d, str):
        if format_string:
            try:
                return datetime.datetime.strptime(d, format_string).date()
            except ValueError:
                return default_value
        try:
            return datetime.date.fromisoformat(d)
        except ValueError:
            return default_value
    return default_value


def _convert_to_datetime(dt, format_string=None, default_value=None):
    # Convert d to a datetime, or to the default value  if connversion fails.  The default_value
    # is a parameter passed in, default to (1900,1,1,0,0,0) (January 1, 1900, 12:00:00 AM)
    # The format_string, if passed, governs conversion from a string.  If there is no format_string,
    # strings are parsed in ISO format.
    # The format string is in the form used by datetime.strptime()
    # Rules:
    #     1. If it's already a datetime, just return the datetime
    #     2. If it's a time, create a datetime with that time and the date portion of the default datetime
    #     3. If it's a date, create a datetime with that date and the time portion of the default datetime
    #     4a. If it's a string and the format_string is provided, use strptime() to parse the string into a datetime
    #     4b. If it's a string and the format_string is not provided, use isoformat() to parse the string into a datetime
    #     4c. If it's a string and parsing fails, return  default_value
    #     5. If all else fails, return the default value
    if default_value is None: default_value = datetime.date(1900, 1, 1, 0, 0, 0)
    if isinstance(dt, datetime.datetime):
        return dt
    if isinstance(dt, datetime.time):
        return datetime.datetime(default_value.year, default_value.month, default_value.day, dt.hour, dt.minute,
                                 dt.second)
    if isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day, default_value.hour, default_value.minute,
                                 default_value.second)
    if isinstance(dt, str):
        if format_string:
            try:
                return datetime.datetime.strptime(dt, format_string)
            except ValueError:
                return default_value
        try:
            return datetime.datetime.fromisoformat(dt)
        except ValueError:
            return default_value
    return default_value


def _coerce_types_in_series(series, data_plane_type, format_string=None, column_default=None):
    # Internal use, coercing a column (from a CSV or a dataframe) to the
    # desired data plane type.
    # ATM, no heroic conversion is being done -- eventually we will have to
    # add a pretty significant ETL component
    if data_plane_type == DATA_PLANE_STRING:
        return [_convert_to_string(x) for x in series]
    if data_plane_type == DATA_PLANE_NUMBER:
        return [_convert_to_number(x, column_default) for x in series]
    if data_plane_type == DATA_PLANE_BOOLEAN:
        return [_convert_to_boolean(b, column_default) for b in series]
    if data_plane_type == DATA_PLANE_TIME_OF_DAY:
        return [_convert_to_time(t, format_string, column_default) for t in series]
    if data_plane_type == DATA_PLANE_DATE:
        return [_convert_to_date(d, format_string, column_default) for d in series]
    if data_plane_type == DATA_PLANE_DATETIME:
        return [_convert_to_datetime(dt, format_string, column_default) for dt in series]


def _convert_default_value(data_plane_type, format_string, default_value):
    # A utility to convert the given default_value to one of the right type.
    # This is because the default_value may come in a string (consider a CSV file)
    # ATM only converts strings
    if default_value is None: return None
    if type(default_value) != str: return default_value
    if data_plane_type == DATA_PLANE_STRING:
        return default_value
    if data_plane_type == DATA_PLANE_NUMBER:
        return _convert_to_number(default_value, None)
    if data_plane_type == DATA_PLANE_BOOLEAN:
        return _convert_to_boolean(default_value, None)
    if data_plane_type == DATA_PLANE_TIME_OF_DAY:
        return _convert_to_time(default_value, format_string, None)
    if data_plane_type == DATA_PLANE_DATE:
        return _convert_to_date(default_value, format_string, None)
    if data_plane_type == DATA_PLANE_DATETIME:
        return _convert_to_datetime(default_value, format_string, None)


def _get_safe(object, key):
    return object[key] if key in object.keys() else None


def convert_element(element, type_conversion_object):
    '''
    convert the element to the appropriate type, using type_conversion_object.
    A type_conversion object is a dictionary with  1-3 members:
          1. type, the type to convert to; (REQUIRED)
          2. format_string, the string to use for formatting (for TIME, DATE, DATETIME types only) (OPTIONAL)
          3. default_value, the default if conversion fails (OPTIONAL)
    Note that if format_string is supplied, and default_value is supplied, and the  type of default_value
    is a string, then the default_value will be converted to the appropriate type.
    For example, {"type": DATA_PLANE_DATE, "format_string": "%m/%d/%Y", "default_value": "12/1//2000"}
    will convert the element to a date, parsing strings as mm/dd/yyyy, and with a default of December 1, 2000
    Parameters:
        element -- the element to be converted
        type_conversion_object: a conversion object as described above
    Returns:
        the element converted into the appropriate type
    '''
    format_string = _get_safe(type_conversion_object, "format_string")
    default_value = _get_safe(type_conversion_object, "default_value")
    data_plane_type = type_conversion_object["type"]
    # Make sure the default_value is the appropriate t ype
    default_value = _convert_default_value(data_plane_type, format_string, default_value)
    if data_plane_type == DATA_PLANE_STRING:
        return _convert_to_string(element)
    if data_plane_type == DATA_PLANE_NUMBER:
        return _convert_to_number(element, default_value)
    if data_plane_type == DATA_PLANE_BOOLEAN:
        return _convert_to_boolean(element, default_value)
    if data_plane_type == DATA_PLANE_TIME_OF_DAY:
        return _convert_to_time(element, format_string, default_value)
    if data_plane_type == DATA_PLANE_DATE:
        return _convert_to_date(element, format_string, default_value)
    if data_plane_type == DATA_PLANE_DATETIME:
        return _convert_to_datetime(element, format_string, default_value)


def convert_row(row, type_conversion_object_list):
    '''
    Convert a row (as a list) to the appropriate types given by the corresponding type_conversion_object
    Parameters:
       row -- the row to be converted
       type_conversion_object_list -- a list (length of the row) of type_conversion_objects to guide the conversion
    '''

    return [convert_element(row[i], type_conversion_object_list[i]) for i in range(len(type_conversion_object_list))]


def convert_column(column, type_conversion_object):
    '''
    Convert a column (as a list) of a DataPlaneTable to the appropriate types given by the corrsponding
    Parameters:
        column -- the column to be converted
        type_conversion_object: a conversion object as described above
    Returns:
        the column converted into the appropriate type

    '''
    format_string = _get_safe(type_conversion_object, "format_string")
    default_value = _get_safe(type_conversion_object, "default_value")
    data_plane_type = type_conversion_object["type"]
    # Make sure the default_value is the appropriate t ype
    default_value = _convert_default_value(data_plane_type, format_string, default_value)
    return _coerce_types_in_series(column, data_plane_type, format_string, default_value)
