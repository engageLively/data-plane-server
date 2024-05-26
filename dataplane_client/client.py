import json

import requests
import pandas as pa
from urllib.parse import urlparse
from data_plane_server.data_plane_server import _log_and_abort
from dataplane.data_plane_utils import InvalidDataException
from data_plane_server.table_server import TableServer, TableNotFoundException, TableNotAuthorizedException, \
    ColumnNotFoundException, build_table_spec
from dataplane.data_plane_table import check_valid_spec


class DataPlaneClient:
    def __init__(self, main_url):
        self.main_url = main_url

    def validate_url(self, main_url):
        parsed_url = urlparse(main_url)

        if not parsed_url.scheme or parsed_url.scheme not in ["http", "https"]:
            raise InvalidDataException(f"Invalid URL scheme: {parsed_url.scheme}")

        if not parsed_url.netloc:
            raise InvalidDataException("URL does not contain a valid network location (netloc)")

    def check_input(self, arg_name):
        """
            3

        """
        if arg_name is not None:
            if not isinstance(arg_name, str):
                raise InvalidDataException(f"{arg_name} must be String")
        else:
            raise ValueError(f"{arg_name} cannot be None")

    def get_all_values(self, table_name, column_name, headers - None):
        """
        Args:
            table_name: name of the table, String
            column_name: name of the column, String
            headers: a dictionary of header variables and values accompanying the request.  
        Returns:
            The  list of column values returned by the data plane server.
        Throws:
            An InvalidDateExeption if the table_name or column_name are malformed, 
            or if the server doesn't have a table of that name
        """

        # Throwing InvalidDataException if entered data is invalid
        try:
            self.check_input(table_name)
            self.check_input(column_name)
        except InvalidDataException as invalid_error:
            _log_and_abort(invalid_error)

        # Throwing Exceptions if there is no table or column found
        try:
            try:
                full_url = f"{self.main_url}/get_all_values?column_name={column_name}&table_name={table_name}"
                self.validate_url(full_url)
                response = requests.get(full_url, headers=headers)
                return response.json()
            except InvalidDataException as invalid_error:
                _log_and_abort(invalid_error)
        except TableNotFoundException:
            _log_and_abort(f'No  table {table_name} present, request /get_all_values', 400)
        except ColumnNotFoundException:
            _log_and_abort(f'No column {column_name} in table {table_name}, request /get_all_values', 400)

    def get_table(self, table_name, headers = None):
        """
        Return the table corresponding to table_name stored on the server
        Args:
            table_name: name of the table to get
            headers:  dictionary of header variables and values accompanying the request
        Returns:
            a DataTable 
        """

    def get_filtered_rows(self, table_name, filter_spec = None, columns = None, headers = None):
        """
            Args:
                table_name: name of the table, String
                filter_spec: Dictionary
                column_name: name of the column, String
            Returns:
                response.json()
        """

        # Throwing InvalidDataException if entered data is invalid
        try:
            self.check_input(table_name)
        except InvalidDataException as invalid_error:
            _log_and_abort(invalid_error)

        # Throwing InvalidDataException if filter_spec is invalid
        if filter_spec is not None:
            try:
                check_valid_spec(filter_spec)
            except InvalidDataException as invalid_error:
                _log_and_abort(invalid_error)

        # Throwing Exceptions if there is no table or column found
        try:
            try:
                full_url = f"{self.main_url}/get_filtered_rows"
                self.validate_url(full_url)
                headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
                response = requests.post(full_url, json={"table": table_name, "filter": filter_spec}, headers=headers)
                response.raise_for_status()
                return response.json()
            except InvalidDataException as invalid_error:
                _log_and_abort(invalid_error)
        except TableNotFoundException:
            _log_and_abort(f'No  table {table_name} present, request /get_filtered_rows', 400)

    def get_range_spec(self, table_name, column_name, headers):
        """
            Args:
                table_name: name of the table, String
                column_name: name of the column, String

            Returns:
                response.json()
        """

        # Throwing InvalidDataException if entered data is invalid
        try:
            self.check_input(table_name)
            self.check_input(column_name)
        except InvalidDataException as invalid_error:
            _log_and_abort(invalid_error)

        # Throwing Exceptions if there is no table or column found
        try:
            try:
                full_url = f"{self.main_url}/get_range_spec?column_name={column_name}&table_name={table_name}"
                self.validate_url(full_url)
                response = requests.get(full_url, headers=headers)
                return response.json()
            except InvalidDataException as invalid_error:
                _log_and_abort(invalid_error)
        except TableNotFoundException:
            _log_and_abort(f'No  table {table_name} present, request /get_range_spec', 400)
        except ColumnNotFoundException:
            _log_and_abort(f'No column {column_name} in table {table_name}, request /get_range_spec', 400)

    def get_tables(self, headers):
        """
            Args:
                None

            Returns:
                response.json()
        """

        # Throwing Exceptions if there is no table found
        try:
            try:
                full_url = f"{self.main_url}/get_tables"
                self.validate_url(full_url)
                response = requests.get(full_url, headers=headers)
                return response.json()
            except InvalidDataException as invalid_error:
                _log_and_abort(invalid_error)
        except TableNotFoundException:
            _log_and_abort(f'No  tables found, request /get_tables', 400)

    def get_table_spec(self, headers):
        """
            Args:
                None

            Returns:
                response.json()
        """

        # Throwing Exceptions if there is no table or column found
        try:
            try:
                full_url = f"{self.main_url}/get_table_spec"
                self.validate_url(full_url)
                response = requests.get(full_url, headers=headers)
                return response.json()
            except InvalidDataException as invalid_error:
                _log_and_abort(invalid_error)
        except TableNotFoundException:
            _log_and_abort(f'No  table tables present, request /get_range_spec', 400)
