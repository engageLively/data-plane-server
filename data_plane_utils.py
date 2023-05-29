'''
Constants and utilities for the data plane
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


""" Types for the data plane schema """

DATA_PLANE_STRING = 'string'
DATA_PLANE_NUMBER = 'number'
DATA_PLANE_BOOLEAN = 'boolean'
DATA_PLANE_DATE = 'date'
DATA_PLANE_DATETIME = 'datetime'
DATA_PLANE_TIME_OF_DAY = 'timeofday'


DATA_PLANE_SCHEMA_TYPES = ['string', 'number', 'boolean', 'date', 'datetime','timeofday']

'''
Exceptions for the Data Plane
'''


# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions."""



class InvalidDataException(Exception):
    '''
    An exception thrown when a data table (list of rows) doesn't match an accoompanying schema,
     or a bad schema is specified, or a table row is the wrong length, or..
    '''
    def __init__(self, message):
        super().__init__()
        self.message = message
