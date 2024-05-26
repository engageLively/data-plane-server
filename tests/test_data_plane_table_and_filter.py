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
from dataplane.data_plane_table import DataPlaneFilter, DataPlaneTable, check_valid_spec, DATA_PLANE_FILTER_FIELDS, DATA_PLANE_FILTER_OPERATORS



# Tests for construction of tables and filters have been completed.  The following
# tests for the actual interaction of tables and filters, namely ensuring that 
# the filters actually filter the data properly.  We will use the following table for tests:




table = DataPlaneTable(schema, lambda:  rows)


def _compare_indices(filter_spec, reference_indices):
    # compare the row indices generated by filter_spec to the reference_indices which
    # should match exactly.  this is the centralized comparison routine for all the 
    # filter tests
    # Parameters:
    #   filter_spec: specification of the filter
    #   reference_indices: row indices to compare against
    data_plane_filter = DataPlaneFilter(filter_spec, schema)
    filter_indices = data_plane_filter.filter_index(rows)
    assert(filter_indices == reference_indices)

# tests.filter_tests.filter_tests is the set of tests over the table_data_good data
# Which was generated by generate_tests.py.  Each test is of the form (spec, expected)
# where spec is a filter_spec and expected is the result

from tests.filter_tests import filter_tests

# For the tests of a particular type (in_list, in_range, regex_match), run all the tests
# by just calling _compare_indices to run the filter and compare the outputs.

def _test_type(type_name):
    for test in filter_tests[type_name]:
        _compare_indices(test["spec"], test["expected"])

# Run all the in_range tests

def test_in_range_filter():
    _test_type("in_range")

# Run all the in_list tests


def test_in_list_filter():
    _test_type("in_list")

# Run all the regex tests

def test_regex_filter():
    _test_type("regex_match")

# Flatten the list of primitive tests to put them all in a single 
# list, where each item is of the form (spec, expected)

def _all_primitive_tests():
    test_list_of_lists = list(filter_tests.values())
    return [item for tests in test_list_of_lists for item in tests]


# Form the list of ALL filters. Check the edge cases (arguments are empty), and then
# check all single arguments and pairs of arguments

def _form_all_filters():
    result = [
        {
            "spec": {"operator": "ALL", "arguments":[]},
            "expected": set(range(len(rows)))
        }
    ]
    test_list = _all_primitive_tests()
    for test in test_list:
        result.append({
            "spec": {"operator": "ALL", "arguments": [test["spec"]]},
            "expected": test["expected"]
        })
        for test1 in test_list:
            result.append({
            "spec": {"operator": "ALL", "arguments": [test["spec"], test1["spec"]]},
            "expected": test["expected"].intersection(test1["expected"])
        })
    return result

# Run the ALL tests -- just form the filters using _form_all_filters and 
# run the tests

def test_all():
    all_tests = _form_all_filters()
    for test in all_tests:
        _compare_indices(test["spec"], test["expected"])

# Form the list of ANY  filters. Check the edge cases (arguments are empty), and then
# check all single arguments and pairs of arguments

def _form_any_filters():
    result = [
        {
            "spec": {"operator": "ANY", "arguments":[]},
            "expected": set()
        }
    ]
    test_list = _all_primitive_tests()
    for test in test_list:
        result.append({
            "spec": {"operator": "ANY", "arguments": [test["spec"]]},
            "expected": test["expected"]
        })
        for test1 in test_list:
            result.append({
            "spec": {"operator": "ANY", "arguments": [test["spec"], test1["spec"]]},
            "expected": test["expected"].union(test1["expected"])
        })
    return result

# Run the ANY tests -- just form the filters using _form_any_filters and 
# run the tests 

def test_any():
    any_tests = _form_any_filters()
    for test in any_tests:
        _compare_indices(test["spec"], test["expected"])

# Form the list of NONE filters. Check the edge cases (arguments are empty), and then
# check all single arguments and pairs of arguments

def _form_none_filters():
    universal = set(range(len(rows)))
    result = [
        {
            "spec": {"operator": "NONE", "arguments":[]},
            "expected": universal
        }
    ]
    test_list = _all_primitive_tests()
    for test in test_list:
        result.append({
            "spec": {"operator": "NONE", "arguments": [test["spec"]]},
            "expected": universal - test["expected"]
        })
        for test1 in test_list:
            result.append({
            "spec": {"operator": "NONE", "arguments": [test["spec"], test1["spec"]]},
            "expected": universal - (test["expected"].union(test1["expected"]))
        })
    return result

# Run the NONE tests -- just form the filters using _form_none_filters and 
# run the tests 


def test_none():
    none_tests = _form_none_filters()
    for test in none_tests:
        _compare_indices(test["spec"], test["expected"])

# Test for a missing filter argument

def test_no_filter():
    all_rows = table.get_filtered_rows()
    assert rows == all_rows

# Test for column selection 

def test_get_columns():
    all_rows = table.get_filtered_rows(columns=['name'])
    names_only = [[name] for name in names]
    assert names_only == all_rows


# Test getting all the values from a filter

def _compare_get_all_values(filter_spec, schema, column_name, expected):
    # Compare the results of get_all_column_values for a filter specification
    # to the result.
    # parameters:
    #   filter_spec: a filter specification
    #   schema: a list of the form {name, type}
    #   column_name: the name of the column to find the values for
    #   expected: the results expected
    data_plane_filter = DataPlaneFilter(filter_spec, schema)
    result = data_plane_filter.get_all_column_values_in_filter(column_name)
    assert(result == expected)

def test_get_all_column_values_in_filter():
    list_filter = {"operator": "IN_LIST", "column": "foo", "values": [1, 2, 3]}
    range_filter = {"operator": "IN_RANGE", "column": "foo", "max_val": 5, "min_val": 4}
    regex_filter = {"operator": "REGEX_MATCH", "column": "bar", "expression": "a.*b"}
    schema = [
        {"name": "foo", "type": DATA_PLANE_NUMBER},
        {"name": "bar", "type": DATA_PLANE_STRING}
    ]
    _compare_get_all_values(list_filter, schema, None, set())
    _compare_get_all_values(list_filter, schema, 1, set())
    _compare_get_all_values(list_filter, schema, "foo", {1, 2, 3})
    _compare_get_all_values(list_filter, schema, "bar", set())
    _compare_get_all_values(range_filter, schema, "foo", {4, 5})
    _compare_get_all_values(regex_filter, schema, "foo", set())
    _compare_get_all_values(regex_filter, schema, "bar", {"a.*b"})
    _compare_get_all_values({"operator": "ALL", "arguments": [list_filter, regex_filter]}, schema, "bar", {"a.*b"})
    _compare_get_all_values({"operator": "ALL", "arguments": [list_filter, regex_filter]}, schema, "foo", {1, 2, 3})
    _compare_get_all_values({"operator": "NONE", "arguments": [list_filter]}, schema, "foo", {1, 2, 3})
    _compare_get_all_values({"operator": "ALL", "arguments": [list_filter, range_filter]}, schema, "foo", {1,2, 3, 4, 5})
