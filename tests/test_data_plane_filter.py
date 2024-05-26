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

import csv
from json import dumps
import math
import re
import random

import pandas as pd
import pytest
from dataplane.data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_STRING, DATA_PLANE_DATE, DATA_PLANE_DATETIME, DATA_PLANE_TIME_OF_DAY, InvalidDataException
from dataplane.data_plane_filter import DataPlaneFilter,  check_valid_spec, DATA_PLANE_FILTER_FIELDS, DATA_PLANE_FILTER_OPERATORS


def _check_valid_spec_error(bad_filter_spec, error_message):
    with pytest.raises(InvalidDataException) as e:
        check_valid_spec(bad_filter_spec)
        assert e.message == error_message

def test_check_valid_spec_bad_operator():
    '''
    Test the routine which checks for an error when a filter_spec is not 
    a dictionary, or is missing an operator
    '''
    for spec in [None, [1, 2, 3], 1, 'a']:
        _check_valid_spec_error(spec, f'filter_spec must be a dictionary, not {type(spec)}' )
    no_operator = {"a": [1, 2, 3]}
    _check_valid_spec_error(no_operator, f'There is no operator in {no_operator}' )
    for bad_operator_type in [None, [1, 2, 3], 1]:
        _check_valid_spec_error({"operator": bad_operator_type}, f'operator {bad_operator_type} is not a string')
    _check_valid_spec_error({"operator": "foo"}, "foo is not a valid operator.  Valid operators are {'ALL', 'ANY', 'NONE', 'IN_LIST', 'IN_RANGE', 'REGEX_MATCH'}")

def _form_test_dict(operator, fields):
    # A little utility to generate a test dictionary for test_missing_fields.
    # this test is going to barf at a missing field, so the values of the fields 
    # don't matter.
    result = {"operator": operator}
    for field in fields: result[field] = "foo"
    return result 

def test_check_valid_spec_missing_fields():
    '''
    Test missing-field errors
    '''
    for operator in DATA_PLANE_FILTER_OPERATORS:
        fields = DATA_PLANE_FILTER_FIELDS[operator]
        for field in fields:
            other_fields = fields - {field}
            test_dict = _form_test_dict(operator, other_fields)
            _check_valid_spec_error(test_dict, f"{operator} is missing fields {[field]}")

def test_check_valid_spec_bad_arguments_field():
    '''
    Check when the 'arguments' field is bad -- None or not a list
    '''
    argument_operators = ['ALL', 'ANY', 'NONE']
    non_lists = [(arg, type(arg)) for arg in [None, 1, 'a', {1, 2}, {"foo": "bar"}]]
    for operator in argument_operators:
        for non_list in non_lists:
            _check_valid_spec_error({"operator": operator, "arguments": non_list[0]}, f'The arguments field for {operator} must be a list, not {non_list[1]}' )


def test_check_valid_spec_bad_in_list_values():
    '''
    Check when the values field to IN_LIST isn't a list of numbers, strings, or booleans
    '''
    bad_values_types = [None, 1, 'a', {"foo"}]
    bad_values_column_checks = [({"operator": "IN_LIST", "column": "a", "values": bad_type}, bad_type) for bad_type in bad_values_types]
    for bad_value in bad_values_column_checks:
        _check_valid_spec_error(bad_value[0], f'The Values argument to IN_LIST must be a list, not {type(bad_value[1])}')
    bad_lists = [([1, [1, 2]], (1, 2)), ([1, {"a"}], {"a"})]
    for bad_value in bad_lists:
        _check_valid_spec_error(bad_value[0], f'Invalid Values {bad_value[1]} for IN_LIST')

def test_check_valid_spec_bad_in_range_values():
    '''
    Check when the max_val and min_val field to IN_RANGE aren't primitives
    '''
    bad_values_types = [None,  {"foo"}, [1, 2, 3]]
    bad_values_column_checks = [({"operator": "IN_RANGE", "column": "a", "max_val": bad_type, "min_val": 2}, bad_type) for bad_type in bad_values_types]
    for bad_value in bad_values_column_checks:
        _check_valid_spec_error(bad_value[0], f'The type of max_val must be one of [str, int, float, bool], not  {type(bad_value[1])}')
    incomparable = [('a', 1), ('a', True),  ('a', 1.0)]
    incomparable_specs =  [({"operator": "IN_RANGE", "column": "a", "max_val": pair[0], "min_val": pair[1]}, f'max_val {pair[0]} and min_val {pair[1]} must be comparable for an IN_RANGE filter') for pair in incomparable]
    for spec in incomparable_specs:
        _check_valid_spec_error(spec[0], spec[1])
    

def test_check_valid_spec_bad_regex():
    '''
    Check when the max_val and min_val aren't numbers
    '''
    bad_regex_types = [None, [1, 2], {"foo"} ]
    bad_regex_checks = [({"operator": "REGEX_MATCH", "column": "a", "expression": bad_type}, bad_type) for bad_type in bad_regex_types]
    for bad_value in bad_regex_checks:
        _check_valid_spec_error(bad_value[0], f'Expression {bad_value[1]} is not a valid regular expression')

def test_check_valid_spec_recursive_arguments():
    '''
    Check for an invalid filter_spec in the arguments of an ANY, ALL, or NONE 
    '''
    operators = ['ANY', 'ALL', 'NONE']
    arguments = [
        {'operator': 'IN_RANGE', 'max_val': 10, 'min_val': 5, 'column': 'age'},
        {'operator': 'IN_LIST', 'values': [1, 2, 3]}
    ]
    error_message = f'{arguments[1]} is missing fields ["column"]'
    for operator in operators:
        _check_valid_spec_error({"operator": operator, "arguments": arguments}, error_message)

import random
def _complex_expression(simple_expression_list):
    if len(simple_expression_list) == 1: return simple_expression_list[0]
    operator = random.choice(['ANY', 'ALL', 'NONE'])
    if len(simple_expression_list) == 2:
        return {"operator": operator, "arguments": simple_expression_list}
    split = random.randrange(1, len(simple_expression_list) - 1)
    arguments = simple_expression_list[:split]
    arguments.append(_complex_expression(simple_expression_list[split:]))
    return {"operator": operator, "arguments": arguments}

VALID_SIMPLE_SPECS = [
    {"operator": "IN_LIST", "column": "name", "values": []},
    {"operator": "IN_LIST",  "column": "foo","values": [True, False]},
    {"operator": "IN_LIST",  "column": "foo","values": [1, 2,  3]},
    {"operator": "IN_LIST",  "column": "name", "values": ["a", "b", "c"]},
    {"operator": "IN_LIST",  "column": 1, "values": ["a", True, 3, 2.5]},
    {"operator": "IN_LIST",  "column": 2, "values": ["a", True, math.nan]},
    {"operator": "IN_RANGE", "column": "age",  "max_val": 1.0, "min_val": 0},
    {"operator": "IN_RANGE", "column": "age", "max_val": 0.0, "min_val": 0},
    {"operator": "IN_RANGE", "column": "age", "max_val": 2, "min_val": 0},
    {"operator": "IN_RANGE", "column": "age", "max_val": 2, "min_val": -4},
    {"operator": "IN_RANGE", "column": "age1","max_val": 2, "min_val": 4},
    {"operator": "IN_RANGE", "column": "age1", "max_val": 2, "min_val": math.nan},
    {"operator": "IN_RANGE", "column": "age1",  "max_val": math.nan, "min_val": math.nan},
    {"operator": "IN_RANGE", "column": "foo", "max_val": True, "min_val": False},
    {"operator": "IN_RANGE", "column": "foo", "max_val": 'a', "min_val": 'b'},
    {"operator": "IN_RANGE", "column": "foo", "max_val": 1, "min_val": True},
    {"operator": "IN_RANGE", "column": "foo", "max_val": 1.0, "min_val": True},
    {"operator": "REGEX_MATCH", "column": "name", "expression": ''},
    {"operator": "REGEX_MATCH", "column": "name", "expression": '^.*$'},
    {"operator": "REGEX_MATCH", "column": "name", "expression": '\\\\'},
    {"operator": "REGEX_MATCH", "column": "name", "expression": '[\\\\]'},
    {"operator": "REGEX_MATCH", "column": "name", "expression": 'foo'},
    {"operator": "REGEX_MATCH", "column": "name", "expression": 'foobar.*$'},
]


def test_check_valid_spec():
    '''
    All of the error cases handled above, now make sure that check_valid_spec accepts all
    valid filter_specs
    '''
    

    for test in VALID_SIMPLE_SPECS:
        check_valid_spec(test)
    
    # form a complex expression and test it:
    check_valid_spec(_complex_expression(VALID_SIMPLE_SPECS))


# We have now completed the tests of check_valid.  For the Filter tests, we'll do one test to 
# ensure that getting an invalid specification really throws an error, then we'll work only with
# valid specifications.

def test_data_plane_filter_invalid_spec():
    '''
    Make sure an invalid spec throws an error
    '''
    with pytest.raises(InvalidDataException) as e:
        DataPlaneFilter(None, ['a', 'b', 'c'])
        # assert e.message == 'filter_spec must be a dictionary, not None'

def _bad_columns_check(filter_spec, columns, expected_message):
    
    with pytest.raises(InvalidDataException) as e:
        DataPlaneFilter(filter_spec, columns)
        # assert e.message == expected_message

def test_data_plane_filter_bad_columns():
    '''
    Test to make sure that if a column name is missing, DataPlaneFilter throws an error
    '''
    filter_spec = {"operator": "IN_LIST", "column": "name", "values": []}
    _bad_columns_check(filter_spec, [1, 2], 'Invalid column specifcations [1, 2]' )
    _bad_columns_check(filter_spec, [{"a": 3, "type": DATA_PLANE_STRING}], 'Invalid column specifications [{"a": 3, "type": DATA_PLANE_STRING}]' )
    _bad_columns_check(filter_spec, [{"name": 3}], 'Invalid column specifcations [{"name": 3}]' )
    _bad_columns_check(filter_spec, [{"name": 3}, {"name": "name", "type": {DATA_PLANE_STRING}}], 'Invalid column specifications [{"name": 3}]' )
    _bad_columns_check(filter_spec, [{"name": "age", "type": DATA_PLANE_NUMBER}], 'name is not a valid column name')
    filter_spec = {"operator": "REGEX_MATCH", "expression": "^.*$", "column": "age"}
    columns = [{"name": "name", "type": DATA_PLANE_STRING}, {"name": "age", "type": DATA_PLANE_NUMBER}]
    _bad_columns_check(filter_spec, columns,  'The column type for a REGEX filter must be DATA_PLANE_STRING, not number')



# Utilities to check each valid filter type

def _check_match(filter_spec, filter):
    assert filter.operator == filter_spec["operator"]
    compound_operators = ['ANY', 'ALL', 'NONE']
    if filter.operator in compound_operators:
        assert len(filter.arguments) == len(filter_spec["arguments"])
        for i in range(len(filter.arguments)):
            _check_match(filter_spec["arguments"][i], filter.arguments[i])
    else:
        assert filter.column_name
        assert filter.column_name == filter_spec["column"]
        if filter.operator == 'REGEX_MATCH':
            assert filter.expression == filter_spec["expression"]
            assert filter.regex == re.compile(filter.expression)

SIMPLE_OVER_COLUMNS =  [spec for spec in VALID_SIMPLE_SPECS if spec["column"] in {"name", "age"}]
COLUMNS_FOR_FILTER_TEST = [
    {"name": "name", "type": DATA_PLANE_STRING},
    {"name": "age", "type": DATA_PLANE_NUMBER},
]


def test_filters_formed_correctly():
    '''
    Test the simple filters are formed correctly for each simple filter spec
    '''
    
    for spec in SIMPLE_OVER_COLUMNS:
        _check_match(spec, DataPlaneFilter(spec, COLUMNS_FOR_FILTER_TEST))

    complex_spec = _complex_expression(SIMPLE_OVER_COLUMNS)
    _check_match(complex_spec, DataPlaneFilter(complex_spec, COLUMNS_FOR_FILTER_TEST))


def test_to_filter_spec():
    '''
    Test to make sure the filter spec generated by a filter corresponds to the 
    input spec
    '''
    for spec in SIMPLE_OVER_COLUMNS:
        filter = DataPlaneFilter(spec, COLUMNS_FOR_FILTER_TEST)
        assert spec == filter.to_filter_spec()

    complex_spec = _complex_expression(SIMPLE_OVER_COLUMNS)
    complex_filter = DataPlaneFilter(complex_spec,  COLUMNS_FOR_FILTER_TEST)
    assert complex_spec == complex_filter.to_filter_spec()

