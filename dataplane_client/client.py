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
            Checks if the input is String
            Args:
                arg_name: argument that is being checked, String

            Returns:
                raises exception if argument is not String

        """
        if arg_name is not None:
            if not isinstance(arg_name, str):
                raise InvalidDataException(f"{arg_name} must be String")
        else:
            raise ValueError(f"{arg_name} cannot be None")

    def get_all_values(self, table_name, column_name, headers):
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

    def get_filtered_rows(self, table_name, filter_spec, headers):
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
            form_data = {
                'table': table_name,
                'filter': filter_spec,
            }
            try:
                full_url = f"{self.main_url}/get_filtered_rows?table_name={table_name}"
                self.validate_url(full_url)
                response = requests.post(full_url, data=form_data, headers=headers)
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
