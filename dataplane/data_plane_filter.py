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
import urllib
import json

from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_DATETIME, DATA_PLANE_DATE, \
    DATA_PLANE_SCHEMA_TYPES, DATA_PLANE_STRING, DATA_PLANE_TIME_OF_DAY
from dataplane.data_plane_utils import InvalidDataException
from dataplane.data_plane_utils import jsonifiable_column, jsonifiable_row, jsonifiable_rows, jsonifiable_value
from dataplane.data_plane_utils import convert_list_to_type, convert_to_type

DATA_PLANE_FILTER_FIELDS = {
    'ALL': {'arguments'},
    'ANY': {'arguments'},
    'NONE': {'arguments'},
    'IN_LIST': {'column', 'values'},
    'IN_RANGE': {'column', 'max_val', 'min_val'},
    'REGEX_MATCH': {'column', 'expression'}
}

DATA_PLANE_FILTER_OPERATORS = set(DATA_PLANE_FILTER_FIELDS.keys())



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
            except ValueError:
                raise InvalidDataException(f'{filter_spec["column"]} is not a valid column name')

            if self.operator == 'IN_LIST':
                self.value_list = convert_list_to_type(self.column_type, filter_spec['values'])
            elif self.operator == 'IN_RANGE':  # operator is IN_RANGE
                max_val = convert_to_type(self.column_type, filter_spec['max_val'])
                min_val = convert_to_type(self.column_type, filter_spec['min_val'])
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
                raise InvalidDataException(f"Filter with operator {self.operator} must have a column name")

            if self.operator == 'IN_LIST':
                result["values"] = jsonifiable_column(self.value_list, self.column_type)
            elif self.operator == 'IN_RANGE':
                result["max_val"] = jsonifiable_value(self.max_val, self.column_type)
                result["min_val"] = jsonifiable_value(self.min_val, self.column_type)
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
