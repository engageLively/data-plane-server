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

from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_DATETIME, DATA_PLANE_DATE, \
    DATA_PLANE_SCHEMA_TYPES, DATA_PLANE_STRING, DATA_PLANE_TIME_OF_DAY, InvalidDataException

DATA_PLANE_FILTER_FIELDS = {
    'ALL': {'arguments'},
    'ANY': {'arguments'},
    'NONE': {'arguments'},
    'IN_LIST': {'column', 'values'},
    'IN_RANGE': {'column', 'max_val', 'min_val'},
    'REGEX_MATCH': {'column', 'expression'}
}

DATA_PLANE_FILTER_OPERATORS = set(DATA_PLANE_FILTER_FIELDS.keys())


def _convert_to_type(data_plane_type, value):
    '''
    Convert value to data_plane_type, so that comparisons can be done.  This is used to convert
    the values in a filter_spec to a form that can be used in a filter.
    Throws an InvalidDataException if the type can't be converted.
    An exception is Boolean, where "True, true, t" are all converted to True, but any
    other values are converted to False

    Arguments:
        data_plane_type: type to convert to
        value: value to be converted
    Returns:
        value cast to the correct type
    '''
    if data_plane_type == DATA_PLANE_STRING:
        if isinstance(value, str):
            return value
        try:
            return str(value)
        except ValueError:
            raise InvalidDataException('Cannot convert value to string')
    elif data_plane_type == DATA_PLANE_NUMBER:
        if isinstance(value, int) or isinstance(value, float):
            return value

        # try an automated conversion to float.  If it fails, it still
        # might be an int in base 2, 8, or 16, so pass the error to try
        # all of those

        try:
            return float(value)
        except ValueError:
            pass
        # if we get here, it must be a string or won't convert
        if not isinstance(value, str):
            raise InvalidDataException(f'Cannot convert {value} to number')

        # Try to convert to binary, octal, decimal

        for base in [2, 8, 16]:
            try:
                return int(value, base)
            except ValueError:
                pass
        # Everything has failed, so toss the exception
        raise InvalidDataException(f'Cannot convert {value} to number')

    elif data_plane_type == DATA_PLANE_BOOLEAN:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value in {'True', 'true', 't', '1'}
        if isinstance(value, int):
            return value == 1
        return False
    # Everything else is a date or time

    elif data_plane_type == DATA_PLANE_DATETIME:
        if type(value) == type(datetime.datetime.now()):
            return value
        if isinstance(value, str):
            try:
                return datetime.datetime.fromisoformat(value)
            except Exception:
                raise InvalidDataException(f"Can't convert {value} to datetime")

    elif data_plane_type == DATA_PLANE_DATE:
        if type(value) == type(datetime.datetime.now().date()):
            return value
        if isinstance(value, str):
            try:
                return datetime.date.fromisoformat(value)
            except Exception:
                raise InvalidDataException(f"Can't convert {value} to date")
    else:  # data_plane_type = DATA_PLANE_TIME_OF_DAT
        if type(value) == type(datetime.datetime.now().time()):
            return value
        if isinstance(value, str):
            try:
                return datetime.time.fromisoformat(value)
            except Exception:
                raise InvalidDataException(f"Can't convert {value} to time")

        raise InvalidDataException(f"Couldn't convert {value} to {data_plane_type}")


def _convert_list_to_type(data_plane_type, value_list):
    '''
    Convert value_list to galyleo_type, so that comparisons can be done.  Currently only works for lists of string, number, and boolean.
    We will add date and time later.
    Returns a default value if value can't be converted
    Note that it's the responsibility of the object which provides the rows to always provide the correct types,
    so this really should always just return a new copy of value_list
    Arguments:
        data_plane_type: type to convert to
        value_list: list of values to be converted
    Returns:
        value_list with each element cast to the correct type
    '''
    return [_convert_to_type(data_plane_type, elem) for elem in value_list]


def _convert_to_output_string(data_plane_type, value):
    '''
    This is the inverse of _convert_to_type: convert a value which is a data plane type to
    a string.  For strings, booleans, and numbers, this is just return the value itself.
    For dates, times, and datetimes, return the ISO format
    Arguments:
        data_plane_type: the type to convert from
        value: the value to convert
    Returns:
        value in a form suitable for a string
    '''
    if data_plane_type in {DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_STRING}:
        return value
    else:
        return value.isoformat()


def _convert_list_to_string(data_plane_type, value_list):
    '''
    This is the inverse of _convert_list_to_type: convert a list of values from data plane type to
    a list of strings.
    Arguments:
        data_plane_type: the type to convert from
        value_list: the value list to convert
    Returns:
        value_list in a form suitable for a list of strings
    '''
    return [_convert_to_output_string(data_plane_type, value) for value in value_list]


def _canonize_set(any_set):
    # Canonize a set into a sorted list; this is useful to ensure that
    # error messages are deterministic
    result = list(any_set)
    result.sort()
    return result


def check_valid_spec(filter_spec):
    '''
    Class method which checks to make sure that a filter spec is valid.
    Does not return, but throws an InvalidDataException with an error message
    if the filter spec is invalid

    Arguments:
        filter_spec: spec to test for validity
    '''

    # Check to make sure filter_spec is a dictionary, and not something else
    if not isinstance(filter_spec, dict):
        raise InvalidDataException(f'filter_spec must be a dictionary, not {type(filter_spec)}')
    #
    # Step 1: check to make sure there is an operator field, and that it's an operator we recognize
    if 'operator' in filter_spec:
        operator = filter_spec['operator']
        valid_operators = ['ALL', 'ANY', 'NONE', 'IN_LIST', 'IN_RANGE', 'REGEX_MATCH']
        if not type(operator) == str:
            raise InvalidDataException(f'operator {operator} is not a string')
        if not operator in valid_operators:
            msg = f'{operator} is not a valid operator.  Valid operators are {valid_operators}'
            raise InvalidDataException(msg)
    else:
        raise InvalidDataException(f'There is no operator in {filter_spec}')
    # Check to make sure that the fields are right for the operator that was given
    # We don't throw an error for extra fields, just for missing fields. Since we're
    # going to use keys() to get the fields in the spec, and this will include the
    # operator, 'operator' is one of the fields

    fields_in_spec = set(filter_spec.keys())
    missing_fields = DATA_PLANE_FILTER_FIELDS[operator] - fields_in_spec
    if len(missing_fields) > 0:
        raise InvalidDataException(f'{filter_spec} is missing required fields {_canonize_set(missing_fields)}')
    # For ALL and ANY, recursively check the arguments list and return
    if (operator in {'ALL', 'ANY', 'NONE'}):
        if not isinstance(filter_spec['arguments'], list):
            bad_type = type(filter_spec["arguments"])
            msg = f'The arguments field for {operator} must be a list, not {bad_type}'
            raise InvalidDataException(msg)
        for arg in filter_spec['arguments']:
            check_valid_spec(arg)
        return
    # if we get here, it's IN_LIST, IN_RANGE, or REGEX_MATCH.

    # For IN_LIST, check that the values argument is a list
    if operator == 'IN_LIST':
        values_type = type(filter_spec['values'])
        if values_type != list:
            msg = f'The Values argument to IN_LIST must be a list, not {values_type}'
            raise InvalidDataException(msg)
    elif operator == 'REGEX_MATCH':

        # check to make sure the expression argument is a valid regex
        try:
            re.compile(filter_spec['expression'])
        except TypeError:
            msg = f'Expression {filter_spec["expression"]} is not a valid regular expression'
            raise InvalidDataException(msg)

    elif operator == 'IN_RANGE':
        primitive_types = {str, int, float, bool}
        # even though dates, datetimes, and times are allowable, they must be strings
        # in iso format in the spec
        '''
        fields = ['max_val', 'min_val']
        for field in fields:
            if filter_spec[field] is None:
                # for some reason type-check doesn't catch this
                raise InvalidDataException(f'The type of {field} must be one of {primitive_types}, not NoneType')

            if not type(filter_spec[field]) in primitive_types:
                raise InvalidDataException(f'The type of {field} must be one of {primitive_types}, not {type(filter_spec[field])}')
        '''

        try:
            # max_val and min_val must be comparable
            result = filter_spec['max_val'] > filter_spec['min_val']
        except TypeError:
            msg = f'max_val {filter_spec["max_val"]} and min_val {filter_spec["min_val"]} must be comparable for an IN_RANGE filter'
            raise InvalidDataException(msg)


def _valid_column_spec(column):
    # True iff column is a dictionary with keys "name", "type"
    if type(column) == dict:
        keys = column.keys()
        return 'name' in keys and 'type' in keys
    return False


class DataPlaneFilter:
    '''
    A Class which implements a Filter used  to filter rows.
    The arguments to the contstructor are a filter_spec, which is a boolean tree
    of filters and the columns which the filter is implemented over.

    This is designed to be instantiated from DataPlaneTable.get_filtered_rows()
    and in no other place -- error checking, if any, should be done there.

    Arguments:
        filter_spec: a Specification of the filter as a dictionary.
        columns: the columns in the form of a list {"name", "type"}
    '''

    def __init__(self, filter_spec, columns):
        check_valid_spec(filter_spec)
        bad_columns = [column for column in columns if not _valid_column_spec(column)]
        if len(bad_columns) > 0:
            raise InvalidDataException(f'Invalid column specifications {bad_columns}')
        self.operator = filter_spec["operator"]
        if (self.operator == 'ALL' or self.operator == 'ANY' or self.operator == 'NONE'):
            self.arguments = [DataPlaneFilter(argument, columns) for argument in filter_spec["arguments"]]
        else:
            column_names = [column["name"] for column in columns]
            column_types = [column["type"] for column in columns]
            try:
                self.column_index = column_names.index(filter_spec["column"])
                self.column_name = column_names[self.column_index]
                self.column_type = column_types[self.column_index]
            except ValueError as original_error:
                raise InvalidDataException(f'{filter_spec["column"]} is not a valid column name')

            if self.operator == 'IN_LIST':
                self.value_list = _convert_list_to_type(self.column_type, filter_spec['values'])
            elif self.operator == 'IN_RANGE':  # operator is IN_RANGE
                max_val = _convert_to_type(self.column_type, filter_spec['max_val'])
                min_val = _convert_to_type(self.column_type, filter_spec['min_val'])
                self.max_val = max_val if max_val >= min_val else min_val
                self.min_val = min_val if min_val <= max_val else max_val
            else:  # operator is REGEX_MATCH
                if column_types[self.column_index] != DATA_PLANE_STRING:
                    raise InvalidDataException(
                        f'The column type for a REGEX filter must be DATA_PLANE_STRING, not {column_types[self.column_index]}')
                # note we've already checked for expression and that it's valid
                self.regex = re.compile(filter_spec['expression'])
                # hang on to the original expression for later jsonification
                self.expression = filter_spec['expression']

    def to_filter_spec(self):
        '''
        Generate a dictionary form of the DataPlaneFilter.  This is primarily for use on the client side, where
        A DataPlaneFilter can be constructed, and then a JSONified form of the dictionary version can be passed to
        the server for server-side filtering.  It's also useful for testing and debugging
        Returns:
            A dictionary form of the Filter
        '''
        compound_operators = {'ALL', 'ANY', 'NONE'}
        result = {"operator": self.operator}
        if self.operator in compound_operators:
            result["arguments"] = [argument.to_filter_spec() for argument in self.arguments]
        else:
            try:
                result["column"] = self.column_name
            except AttributeError as e:
                print(result)

            if self.operator == 'IN_LIST':
                result["values"] = _convert_list_to_string(self.column_type, self.value_list)
            elif self.operator == 'IN_RANGE':
                result["max_val"] = _convert_to_output_string(self.column_type, self.max_val)
                result["min_val"] = _convert_to_output_string(self.column_type, self.min_val)
            else:  # operator == 'REGEX_MATCH'
                result["expression"] = self.expression
        return result

    def filter(self, rows):
        '''
        Filter the rows according to the specification given to the constructor.
        Returns the rows for which the filter returns True.

        Arguments:
            rows: list of list of values, in the same order as the columns
        Returns:
            subset of the rows, which pass the filter
        '''
        # Just an overlay on filter_index, which returns the INDICES of the rows
        # which pass the filter.  This is the top-level call, filter_index is recursive
        indices = self.filter_index(rows)
        return [rows[i] for i in range(len(rows)) if i in indices]

    def filter_index(self, rows):
        '''
        Not designed for external call.
        Filter the rows according to the specification given to the constructor.
        Returns the INDICES of the  rows for which the filter returns True.
        Arguments:

            rows: list of list of values, in the same order as the columns

        Returns:
            INDICES of the rows which pass the filter, AS A SET

        '''
        all_indices = range(len(rows))
        if self.operator == 'ALL':
            argument_indices = [argument.filter_index(rows) for argument in self.arguments]
            return reduce(lambda x, y: x & y, argument_indices, set(all_indices))
        elif self.operator == 'ANY':
            argument_indices = [argument.filter_index(rows) for argument in self.arguments]
            return reduce(lambda x, y: x | y, argument_indices, set())
        elif self.operator == 'NONE':
            argument_indices = [argument.filter_index(rows) for argument in self.arguments]
            return reduce(lambda x, y: x - y, argument_indices, set(all_indices))
        # Primitive operator if we get here.  Dig out the values to filter
        values = [row[self.column_index] for row in rows]
        if self.operator == 'IN_LIST':
            return set([i for i in all_indices if values[i] in self.value_list])
        elif self.operator == 'IN_RANGE':
            return set([i for i in all_indices if values[i] <= self.max_val and values[i] >= self.min_val])
        else:  # self.operator == 'REGEX_MATCH'
            return set([i for i in all_indices if self.regex.fullmatch(values[i]) is not None])

    def get_all_column_values_in_filter(self, column_name):
        '''
        Return the set of all values in operators for column column_name.  This is for the 
        case where a back-end process (e.g., a SQL stored procedure) takes parameter values
        for specific columns, and we want to use the values here to select at the source.
        Arguments:
             column_name: the name of the column to get all the values for
        Returns:
             A SET of all of the values for the column
        '''
        if column_name is None:
            return set()
        if type(column_name) != str:
            return set()
        if self.operator in ['ALL', 'ANY', 'NONE']:
            values = set()
            # I'd like to use a comprehension here, but I'm not sure how it interacts with union
            for argument in self.arguments:
                values = values.union(argument.get_all_column_values_in_filter(column_name))
            return values
        if column_name != self.column_name:
            return set()
        if self.operator == 'IN_LIST':
            return set(self.value_list)
        if self.operator == 'IN_RANGE':
            return {self.max_val, self.min_val} 
        # must be REGEX_MATCH
        return {self.expression}
        
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


class DataPlaneTable:
    '''
    A DataPlaneTable: This is instantiated with a function get_rows() which  delivers the
    rows, rather than having them explicitly in the Table.  Note that get_rows() *must* return
    the appropriate number of columns of the appropriate types.

    Arguments:
        schema: a list of records of the form {"name": <column_name, "type": <column_type>}.
           The column_type must be a type from galyleo_constants.DATA_PLANE_TYPES.
        get_rows: a function which returns a list of list of values.  Each component list
            must have the same length as schema, and the jth element must be of the
            type specified in the jth element of schema
        header_variables: a dictionary of two fields, required and optional,
            both of which are lists of strings (variable names).  Either
            or both can be empty.  The variables (and their values) are
            passed with each table request
    '''

    def __init__(self, schema, get_rows, header_variables=None):
        self.is_dataplane_table = True
        self.schema = schema
        self.get_rows = get_rows
        self.header_variables = DEFAULT_HEADER_VARIABLES if header_variables is None else header_variables

    # This is used to get the names of a column from the schema

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

    def all_values(self, column_name: str):
        '''
        get all the values from column_name
        Arguments:

            column_name: name of the column to get the values for

        Returns:
            List of the values

        '''
        try:
            index = self.column_names().index(column_name)
        except ValueError as original_error:
            raise InvalidDataException(f'{column_name} is not a column of this table') from original_error
        data_plane_type = self.schema[index]["type"]
        rows = self.get_rows()
        result = _convert_list_to_type(data_plane_type, list(set([row[index] for row in rows])))
        result.sort()
        return result

    def range_spec(self, column_name: str):
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

        values = self.all_values(column_name)

        return {"max_val": values[-1], "min_val": values[0]}

    def get_filtered_rows(self, filter_spec=None, columns=[]):
        '''
        Filter the rows according to the specification given by filter_spec.
        Returns the rows for which the resulting filter returns True.

        Arguments:
            filter_spec: Specification of the filter, as a dictionary
            columns: the names of the columns to return.  Returns all columns if absent
        Returns:
            The subset of self.get_rows() which pass the filter
        '''
        # Note that we don't check if the column names are all valid
        if columns is None: columns = []  # Make sure there's a value
        if filter_spec is None:
            rows = self.get_rows()
        else:
            made_filter = DataPlaneFilter(filter_spec, self.schema)
            rows = made_filter.filter(self.get_rows())
        if columns == []:
            return rows
        else:
            names = self.column_names()
            column_indices = [i for i in range(len(names)) if names[i] in columns]
            return [_select_entries_from_row(row, column_indices) for row in rows]


class RowTable(DataPlaneTable):
    '''
    A simple utility class to serve data from a static list of rows, which
    can be constructed from a CSV file, Excel File, etc.  The idea is to
    make it easy for users to create and upload simple datasets to be
    served from a general-purpose server.  Note that this will need some
    authentication.
    '''

    def __init__(self, schema, rows):
        super(RowTable, self).__init__(schema, self._get_rows)
        self.rows = rows

    def _get_rows(self):
        '''
        Very simple: just return the rows
        '''
        return self.rows


def _convert_to_string(s):
    return s if isinstance(s, str) else str(s)


def _convert_to_number(x, default_value=nan):
    # Convert x to a number, or nan if there is no conversion
    if (isinstance(x, int) or isinstance(x, float)):
        return x
    try:
        return float(x)
    except ValueError:
        pass
    try:
        return int(x)
    except ValueError:
        return default_value


def _convert_to_time(t, format_string=None, default_value=datetime.time(0, 0, 0)):
    if isinstance(t, datetime.time):
        return t
    if isinstance(t, datetime.date):
        return default_value
    if isinstance(t, datetime.datetime):
        return t.time()
    if isinstance(t, str):
        if format_string:
            try:
                return datetime.dateime.strptime(t, format_string).time()
            except ValueError:
                return default_value
        try:
            return datetime.time.fromisoformat(t)
        except ValueError:
            return default_value
    return default_value


def _convert_to_date(d, format_string=None, default_value=datetime.date(1900, 1, 1)):
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


def _convert_to_datetime(dt, format_string=None, default_value=datetime.datetime(1900, 1, 1, 0, 0, 0)):
    if isinstance(dt, datetime.datetime):
        return dt
    if isinstance(dt, datetime.time):
        return datetime.datetime(default_value.year, default_value.month, default_value.day, dt.hour, dt.minute,
                                 dt.second)
    if isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day, 0, 0, 0)
    if isinstance(dt, str):
        if format_string:
            try:
                return datetime.dateime.strptime(dt, format_string)
            except ValueError:
                return default_value
        try:
            return datetime.datetime.fromisoformat(dt)
        except ValueError:
            return default_value
    return default_value


def _coerce_types(series, data_plane_type, column_default=None):
    # Internal use, coercing a series (from a CSV or a dataframe) to the
    # desired data plane type.
    # ATM, no heroic conversion is being done -- eventually we will have to
    # add a pretty significant ETL component
    if data_plane_type == DATA_PLANE_STRING:
        return [_convert_list_to_string(x) for x in series]
    if data_plane_type == DATA_PLANE_NUMBER:
        return [_convert_to_number(x) for x in series]
    if data_plane_type == DATA_PLANE_BOOLEAN:
        return [True if b else False for b in series]
    if data_plane_type == DATA_PLANE_TIME_OF_DAY:
        return [_convert_to_time(t) for t in series]
    if data_plane_type == DATA_PLANE_DATE:
        return [_convert_to_date(d) for d in series]
    if data_plane_type == DATA_PLANE_DATETIME:
        return [_convert_to_datetime(dt) for dt in series]


class RemoteCSVTable(DataPlaneTable):
    '''
    A very common format for data interchange on the Internet is a downloadable
    CSV file.  It's so common it's worth making a class, just for this.  The
    idea is that, when a get_rows request comes in, we download the table
    into a dataframe and then return the list of rows, perhaps after doing
    unit conversion
    '''

    def __init__(self, schema, url):
        super(schema, self.get_rows)
        self.url = url
        self.dataframe = None

    def get_rows(self):
        if self.dataFrame is None:
            self.dataframe = pd.read_csv(self.url)
        return self.dataframe.values.tolist()
