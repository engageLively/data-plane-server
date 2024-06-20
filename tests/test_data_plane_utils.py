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

import pytest
import datetime
import os
from dataplane.data_plane_utils import *
import math
good_types = {
  DATA_PLANE_STRING: ["foo"],
  DATA_PLANE_NUMBER: [3.2, 32],
  DATA_PLANE_BOOLEAN: [True, False],
  DATA_PLANE_DATE: [datetime.date(2021, 1, 1)],
  DATA_PLANE_DATETIME: [datetime.datetime(2021, 1, 1, 12, 0, 0)],
  DATA_PLANE_TIME_OF_DAY: [datetime.time(12, 0, 0)]
}

# Test check_valid for good and bad types
def test_type_check_good():
  for key, item_list in good_types.items():
    for item in item_list: assert(type_check(key, item))

bad_types = {
  DATA_PLANE_STRING: [None, 3.2, 32, True, False, datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 12, 0, 0), datetime.time(12, 0, 0)],
  DATA_PLANE_NUMBER: [None, "foo", True, False, datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 12, 0, 0), datetime.time(12, 0, 0)],
  DATA_PLANE_BOOLEAN: [None, 3.2, 32, "foo", datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 12, 0, 0), datetime.time(12, 0, 0)],
  DATA_PLANE_DATE: [None, 3.2, 32, True, False, "foo", datetime.datetime(2021, 1, 1, 12, 0, 0), datetime.time(12, 0, 0)],
  DATA_PLANE_DATETIME: [None, 3.2, 32, True, False,  datetime.date(2021, 1, 1), "foo", datetime.time(12, 0, 0)],
  DATA_PLANE_TIME_OF_DAY: [None, 3.2, 32, True, False, datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 12, 0, 0), "foo"]
}

def test_type_check_bad():
  for key, item_list in bad_types.items():
    for item in item_list: assert(not type_check(key, item))

# Check the list; since we know that type_sest works, this just makes sure it's right on a list of 
# length 0, 1 > 1
def test_check_type_of_list():
  assert(check_dataplane_type_of_list(DATA_PLANE_STRING, []))
  assert(check_dataplane_type_of_list(DATA_PLANE_STRING, ["foo"]))
  assert(check_dataplane_type_of_list(DATA_PLANE_STRING, ["foo", "bar"]))
  assert(not check_dataplane_type_of_list(DATA_PLANE_STRING, [32]))
  assert(not check_dataplane_type_of_list(DATA_PLANE_STRING, ["foo", 32]))

jsonifiable_values = {
  DATA_PLANE_STRING: [("foo", "foo")],
  DATA_PLANE_NUMBER: [(3.2, 3.2), (32, 32)],
  DATA_PLANE_BOOLEAN: [(True, True), (False, False)],
  DATA_PLANE_DATE: [(datetime.date(2021, 1, 1), "2021-01-01")],
  DATA_PLANE_DATETIME: [(datetime.datetime(2021, 1, 1, 12, 0, 0), "2021-01-01T12:00:00")],
  DATA_PLANE_TIME_OF_DAY: [(datetime.time(12, 0, 0), "12:00:00")]
}

# Check jsonifiable values

def test_jsonifiable_value():
  for (data_plane_type, translate_list) in jsonifiable_values.items():
    for (value, json_value) in translate_list:
      assert(jsonifiable_value(value, data_plane_type) == json_value)

# Check a jsonifiable row.  Since we know the values convert properly, this is just a 
# test on first, rows, and then lists of rows

jsonifiable_rows_test = [
  ([datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 12, 0, 0)], ["2021-01-01", "2021-01-01T12:00:00"]),
  ([datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 12, 0, 0)], ["2021-01-01", "2021-01-01T12:00:00"])
]

def test_jsonifiable_row():
  for row in jsonifiable_rows_test:
    assert(jsonifiable_row(row[0], [DATA_PLANE_DATE, DATA_PLANE_DATETIME]) == row[1])

def test_jsonifiable_rows():
  rows = [jsonifiable_rows_test[0][0], jsonifiable_rows_test[1][0]]
  solutions = [jsonifiable_rows_test[0][1], jsonifiable_rows_test[1][1]]
  assert(jsonifiable_rows(rows, [DATA_PLANE_DATE, DATA_PLANE_DATETIME]) == solutions)

# Check a jsonifiable column.  Since we know the values convert properly, this is just a 
# test on individual columns.  STRING, NUMBER, and BOOLEAN just return the identity,
# DATE, TIME, and DATETIME convert to strings

def test_jsonifiable_column():
  assert(jsonifiable_column([3.2, 2], DATA_PLANE_NUMBER) == [3.2, 2])
  assert(jsonifiable_column([datetime.datetime(2021, 1, 1, 12, 0, 0), datetime.datetime(2021, 1, 1, 12, 1, 1)], DATA_PLANE_DATETIME) == ["2021-01-01T12:00:00", "2021-01-01T12:01:01"])
  assert(jsonifiable_column([datetime.time(12, 0, 0), datetime.time(12, 1, 1)], DATA_PLANE_TIME_OF_DAY) == ["12:00:00", "12:01:01"])
  assert(jsonifiable_column([datetime.date(2021, 1, 1), datetime.date(2021, 2, 1)], DATA_PLANE_DATE) == ["2021-01-01", "2021-02-01"])
  
# Test value conversion. 

# 1. Strings.  

def test_convert_string():
  results = [('foo', 'foo'), (1, '1'), (1.2, '1.2'), ({'a': 1, 'b':[1, 2, 3]}, "{'a': 1, 'b': [1, 2, 3]}"), (None, 'None')]
  for test in results:
    assert(convert_to_type(DATA_PLANE_STRING, test[0]) == test[1])

# Numbers.  Special cases:
# all strings convert to floats
# nans convert to nans
# errors throw InvalidDataException

def test_convert_number():
  results = [(1, 1), (1.2, 1.2),  ('1', 1.0)]
  for test in results:
    assert(convert_to_type(DATA_PLANE_NUMBER, test[0]) == test[1])
  nans = [math.nan, 'nan']
  for nan in nans:
    assert(math.isnan(convert_to_type(DATA_PLANE_NUMBER, nan)))
  exceptions = [None, 'foo', [1, 2]]
  for err in exceptions:
    with pytest.raises(InvalidDataException) as exc:
      convert_to_type(DATA_PLANE_NUMBER, err)
      assert(repr(exc) == f'Cannot convert {err} to number')

# Booleans.  Special cases:
# {'True', 'true', 't', '1'} convert to True
# Nonzero numbers convert to True
# everything else converts to False
def test_convert_bool():
  assert(convert_to_type(DATA_PLANE_BOOLEAN, True))
  assert(not convert_to_type(DATA_PLANE_BOOLEAN, False))
  true_vals = {'True', 'true', 't', '1', '1.0'}
  for val in true_vals:
    assert(convert_to_type(DATA_PLANE_BOOLEAN, val))
  numbers = [1, 1.0, 0.1, 5, 10, -3, math.nan]
  for num in numbers:
    assert(convert_to_type(DATA_PLANE_BOOLEAN, num))
  false_vals  = [None, 0, 0.0, '', [1, 2, 3], "f"]
  for false in false_vals:
    assert(not convert_to_type(DATA_PLANE_BOOLEAN, false))

# Datetimes.  The only valid datetimes are datetimes, dates, and isoformat strings
# that are read by datetime.datetime or datetime.date
def test_convert_datetime():
  valid = [
    (datetime.datetime(1, 1, 1, 1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1)),
    (datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 0, 0, 0)),
    ('2021-01-01T00:01:00',datetime.datetime(2021, 1, 1, 0, 1, 0)),
    ('2021-01-01', datetime.datetime(2021, 1, 1, 0, 0, 0))
  ]
  for v in valid:
    assert(convert_to_type(DATA_PLANE_DATETIME, v[0]) == v[1])
  invalid_strings = ['foo', '2021', '2021-01', '2021-1-1', '2021-01-01T0:0:0']
  for iv in invalid_strings:
    with pytest.raises(InvalidDataException) as exc:
      convert_to_type(DATA_PLANE_DATETIME, iv)
      assert(repr(exc) == f'Cannot convert {iv} to datetime')
  invalid = [None, 1, [1, 2, 3], True]
  for iv in invalid:
    with pytest.raises(InvalidDataException) as exc:
      convert_to_type(DATA_PLANE_DATETIME, iv)
      assert(repr(exc) == f'Cannot convert {iv} to datetime')
  
# Dates.  The only valid dates are datetimes, dates, and isoformat strings
# that are read by datetime.datetime
def test_convert_date():
  valid = [
    (datetime.datetime(1, 1, 1, 1, 1, 1), datetime.date(1, 1, 1)),
    (datetime.date(1, 1, 1), datetime.date(1, 1, 1)),
    ('2021-01-01T00:01:00',datetime.date(2021, 1, 1)),
    ('2021-01-01', datetime.date(2021, 1, 1))
  ]
  for v in valid:
    assert(convert_to_type(DATA_PLANE_DATE, v[0]) == v[1])
  invalid_strings = ['foo', '2021', '2021-01', '2021-1-1', '2021-01-01T0:0:0']
  for iv in invalid_strings:
    with pytest.raises(InvalidDataException) as exc:
      convert_to_type(DATA_PLANE_DATE, iv)
      assert(repr(exc) == f'Cannot convert {iv} to date')
  invalid = [None, 1, [1, 2, 3], True]
  for iv in invalid:
    with pytest.raises(InvalidDataException) as exc:
      convert_to_type(DATA_PLANE_DATE, iv)
      assert(repr(exc) == f'Cannot convert {iv} to date')
  
# Times.  The only valid datetimes are datetimes, times, and isoformat strings
# that are read by datetime.datetime 
def test_convert_times():
  valid = [
    (datetime.datetime(1, 1, 1, 1, 1, 1), datetime.time(1, 1, 1)),
    (datetime.time(1, 1, 1), datetime.time(1, 1, 1)),
    (datetime.time(1, 1), datetime.time(1, 1, 0)),
    (datetime.time(1), datetime.time(1, 0, 0)),
    ('2021-01-01T00:01:00',datetime.time(0, 1, 0)),
    ('2021-01-01',datetime.time(0, 0, 0)),
    ('00:01:00', datetime.time(0, 1, 0))
  ]
  for v in valid:
    assert(convert_to_type(DATA_PLANE_TIME_OF_DAY, v[0]) == v[1])
  invalid_strings = ['foo', '0:0:0', '0', '2021-01-01T0:0:0']
  for iv in invalid_strings:
    with pytest.raises(InvalidDataException) as exc:
      convert_to_type(DATA_PLANE_TIME_OF_DAY, iv)
      assert(repr(exc) == f'Cannot convert {iv} to date')
  invalid = [None, 1, [1, 2, 3], True]
  for iv in invalid:
    with pytest.raises(InvalidDataException) as exc:
      convert_to_type(DATA_PLANE_TIME_OF_DAY, iv)
      assert(repr(exc) == f'Cannot convert {iv} to date')
  
  
# Test convert_list_to_type.  Since we've already tested value conversions, the only tests are 
# how it performs on lists and special cases.  The cases to test are:
# 1. None
# 2. empty list
# 3. All correct
# 4. error in list  

def test_convert_list_to_type():
  conversions = [([], []), ([True, 1], [True, True])]
  for conversion in conversions:
    assert(convert_list_to_type(DATA_PLANE_BOOLEAN, conversion[0]) == conversion[1])
  exceptions = [
    (None, DATA_PLANE_BOOLEAN, 'Failed to convert None to boolean'),
    ([1, 2, "foo"], DATA_PLANE_NUMBER, 'Failed to convert [1, 2, "foo"] to number')
  ]
  for exception in exceptions:
    with pytest.raises(InvalidDataException) as exc:
      convert_list_to_type(exception[1], exception[0])
      assert(repr(exc) == exception[2])

# Test convert_dict_to_type.  Since we've already tested value conversions, the only tests are 
# how it performs on dictionaries.  
# 
def test_convert_dict_to_type():
  conversions = [
    ({"a": "a"}, DATA_PLANE_STRING, {"a": "a"}),
    ({"a": 1}, DATA_PLANE_NUMBER, {"a": 1}),
    ({"a": '2024-01-01'}, DATA_PLANE_DATE, {"a": datetime.date(2024, 1, 1)}),
    ({"a": '2024-01-01T12:00:01'}, DATA_PLANE_DATETIME, {"a": datetime.datetime(2024, 1, 1, 12, 0, 1)}),
    ({"a": '12:00:01'}, DATA_PLANE_TIME_OF_DAY, {"a": datetime.time(12, 0, 1)}),
    ({"a": 1}, DATA_PLANE_BOOLEAN, {"a": True}),
    ({}, DATA_PLANE_DATETIME, {})
  ]
  for conversion in conversions:
    assert(convert_dict_to_type(conversion[1], conversion[0]) == conversion[2])
  
  non_dicts = [None, {"a"}, "a", [1, 2, 3]]
  for non_dict in non_dicts:
    with pytest.raises(InvalidDataException) as exc:
      convert_dict_to_type(non_dict, DATA_PLANE_BOOLEAN) # Type doesn't matter
      assert(repr(exc) == f'Failed to conver {non_dict} to {DATA_PLANE_BOOLEAN}')