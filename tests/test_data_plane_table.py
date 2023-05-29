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

import pandas as pd
import pytest
from data_plane_utils import DATA_PLANE_BOOLEAN, DATA_PLANE_NUMBER, DATA_PLANE_STRING, InvalidDataException
from data_plane_table import DataPlaneFilter, DataPlaneTable, check_valid_spec, DATA_PLANE_FILTER_FIELDS, DATA_PLANE_FILTER_OPERATORS

table_test_1 = {
    "rows": [["Ted", 21], ["Alice", 24]],
    "schema": [
        {"name": "name", "type": DATA_PLANE_STRING},
        {"name": "age", "type": DATA_PLANE_NUMBER}
    ]
}

def _makeTable():
    return  DataPlaneTable(table_test_1["schema"], lambda: table_test_1["rows"])

def test_create():
    '''
    Test table creation and ensure that the names and types match
    '''
    table = _makeTable()
    assert table.column_names() == ['name', 'age']
    assert table.column_types() == [DATA_PLANE_STRING, DATA_PLANE_NUMBER]
    assert table.get_rows() == table_test_1["rows"]
    for column in table_test_1["schema"]:
        assert(table.get_column_type(column["name"]) == column["type"])
    assert table.get_column_type(None) == None
    assert table.get_column_type("Foo") == None




def test_all_values_and_numeric_spec():
    '''
    Test getting all the values and the numeric specification from columns
    '''
    table = _makeTable()
    assert table.all_values('name') == ['Alice', 'Ted']
    assert table.all_values('age') == [21, 24]
    with pytest.raises(InvalidDataException) as e:
        table.all_values(None)
        assert repr(e) == 'None is not a column of this table'
    with pytest.raises(InvalidDataException) as e:
        table.all_values('Foo')
        assert repr(e) == 'Foo is not a column of this table'
    with pytest.raises(InvalidDataException) as e:
        table.numeric_spec(None)
        assert repr(e) == 'None is not a column of this table'
    with pytest.raises(InvalidDataException) as e:
        table.numeric_spec('Foo')
        assert repr(e) == 'Foo is not a column of this table'
    with pytest.raises(InvalidDataException) as e:
        table.numeric_spec('name')
        assert repr(e) == f'The type of name must be {DATA_PLANE_NUMBER}, not {DATA_PLANE_STRING}'
    assert table.numeric_spec('age') == {'max_val': 24, "min_val": 21, "increment": 3}
    table.get_rows = lambda: [['Ted', 21], ['Alice', 24], ['Jane', 'foo']]
    with pytest.raises(InvalidDataException) as e:
        table.numeric_spec('age')
        assert repr(e) == 'Bad data in column age'
    table.get_rows = lambda: [['Ted', 21], ['Alice', 24], ['Jane', 20]]
    assert table.numeric_spec('age') == {'max_val': 24, "min_val": 20, "increment": 1}


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
    Check when the max_val and min_val aren't numbers
    '''
    bad_values_types = [None, [1, 2], 'a', {"foo"} ]
    for fields in [("max_val", "min_val"), ("min_val, max_val")]:
        bad_values_max_checks = [({"operator": "IN_RANGE", "column": "a", fields[0]: bad_type, fields[1]: 1}, bad_type) for bad_type in bad_values_types]
        for bad_value in bad_values_max_checks:
            _check_valid_spec_error(bad_value[0], f'The type of {fields[0]} for IN_RANGE must be a number, not {type(bad_value[1])}')
    

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


def test_check_valid_spec():
    '''
    All of the error cases handled above, now make sure that check_valid_spec accepts all
    valid filter_specs
    '''
    valid_simple = [
        {"operator": "IN_LIST", "column": "name", "values": []},
        {"operator": "IN_LIST",  "column": "name","values": [True, False]},
        {"operator": "IN_LIST",  "column": "name","values": [1, 2,  3]},
        {"operator": "IN_LIST",  "column": "name", "values": ["a", "b", "c"]},
        {"operator": "IN_LIST",  "column": 1, "values": ["a", True, 3, 2.5]},
        {"operator": "IN_LIST",  "column": 2, "values": ["a", True, math.nan]},
        {"operator": "IN_RANGE", "column": "age",  "max_val": 1.0, "min_val": 0},
        {"operator": "IN_RANGE", "column": "age", "max_val": 0.0, "min_val": 0},
        {"operator": "IN_RANGE", "column": "age", "max_val": 2, "min_val": 0},
        {"operator": "IN_RANGE", "column": "age", "max_val": 2, "min_val": -4},
        {"operator": "IN_RANGE", "column": "age","max_val": 2, "min_val": 4},
        {"operator": "IN_RANGE", "column": "age", "max_val": 2, "min_val": math.nan},
        {"operator": "IN_RANGE", "column": "age",  "max_val": math.nan, "min_val": math.nan},
        {"operator": "REGEX_MATCH", "column": "name", "expression": ''},
        {"operator": "REGEX_MATCH", "column": "name", "expression": '^.*$'},
        {"operator": "REGEX_MATCH", "column": "name", "expression": '\\\\'},
        {"operator": "REGEX_MATCH", "column": "name", "expression": '[\\\\]'},
        {"operator": "REGEX_MATCH", "column": "name", "expression": 'foo'},
        {"operator": "REGEX_MATCH", "column": "name", "expression": 'foobar.*$'},
    ]

    for test in valid_simple:
        check_valid_spec(test)
    
    # form a complex expression and test it:
    check_valid_spec(_complex_expression(valid_simple))


