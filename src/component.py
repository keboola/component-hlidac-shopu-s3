"""
Template Component main class.

"""
import csv
import json
import logging
from pathlib import Path
from typing import List
import os
import shutil

from csv2json.hone_csv2json import Csv2JsonConverter
from keboola.component.base import ComponentBase
from keboola.component.dao import TableDefinition, FileDefinition
from keboola.component.exceptions import UserException
from uploader.client import S3Writer

# configuration variables
KEY_FORMAT = 'format'
AWS_SECRET_ACCESS_KEY = '#aws_secret_access_key'
AWS_ACCESS_KEY_ID = 'aws_access_key_id'
AWS_BUCKET = "aws_bucket"
S3_BUCKET_DIR = "aws_directory"
CHUNKSIZE = "chunksize"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [AWS_SECRET_ACCESS_KEY,
                       AWS_ACCESS_KEY_ID,
                       AWS_BUCKET,
                       KEY_FORMAT]

coltypes_metadata = [{"column": "itemId",
                      "type": "string"},
                     {"column": "itemName",
                      "type": "string"},
                     {"column": "itemImage",
                      "type": "string"}]


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.upload_processor = None
        self.s3_bucket_dir = ''
        self.params = None
        self.target_paths = None
        self.local_paths = None
        self.chunksize = 5000

    def run(self):
        """
        Main execution code
        """
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        self.s3_bucket_dir = params.get(S3_BUCKET_DIR)

        # Access parameters in data/config.json
        if params.get(KEY_FORMAT):
            logging.info(f"Format setting is: {params.get(KEY_FORMAT)}")

        if params.get(CHUNKSIZE):
            self.chunksize = int(params.get(CHUNKSIZE))
            logging.info(f"Chunk size set to: {self.chunksize}")
        else:
            logging.warning(f"Chunk size is not set. Using default chunksize: {self.chunksize}.")

        input_tables = self.get_input_tables_definitions()

        self.upload_processor = S3Writer(params, self.files_out_path)

        if not self.upload_processor.test_connection_ok(params):
            logging.warning("Connection check failed. Connection is not possible or your account does not have "
                            "READ_ACP rights.")

        for table in input_tables:
            _format = self.configuration.parameters[KEY_FORMAT]
            logging.info(f"Processing table {table.name} using format: {_format}")
            if _format == 'pricehistory':
                self._generate_price_history(table)
            elif _format == 'metadata':
                self._generate_metadata(table)
            else:
                raise UserException(f"Wrong parameter in data/config.json {_format}. "
                                    "Viable parameters are: pricehistory/metadata")

        logging.info("Parsing finished successfully!")

    def _validate_expected_columns(self, table_type, table: TableDefinition, expected_columns: List[str]):
        errors = []
        # validate
        for c in expected_columns:
            if c not in table.columns:
                errors.append(c)

        if errors:
            error = f'Some required columns are missing for format {table_type}. ' \
                    f'Missing columns: [{"; ".join(errors)}] '
            raise UserException(error)

    def output_folder_cleanup(self) -> None:
        dir_to_clean = self.files_out_path
        for files in os.listdir(dir_to_clean):
            path = os.path.join(dir_to_clean, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)

    def _write_json_content_to_file(self, file: FileDefinition, content: dict):
        Path(file.full_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file.full_path, 'w+', encoding='utf-8') as outp:
            json.dump(content, outp)

    def _generate_price_history(self, table: TableDefinition):
        expected_columns = ['shop_id', 'slug', 'json']
        # validate
        self._validate_expected_columns('pricehistory', table, expected_columns)

        with open(table.full_path, 'r', encoding='utf-8') as inp:
            reader = csv.DictReader(inp)
            i = 0  # inside-chunk counter
            for row in reader:
                out_file = self.create_out_file_definition(f'{row["shop_id"]}/{row["slug"]}/price-history.json')
                content = json.loads(row['json'])
                self._write_json_content_to_file(out_file, content)
                i += 1
                if i == self.chunksize:
                    logging.info(f"Uploading chunk for table {table.name} to S3")
                    # CREATE LIST OF FILES IN OUTPUT FOLDER
                    self.local_paths, self.target_paths = self.upload_processor.prepare_lists_of_files(
                        self.files_out_path,
                        self.s3_bucket_dir)
                    # SEND FILES TO TARGET DIR IN S3
                    self.upload_processor.process_upload(self.local_paths, self.target_paths)

                    # DELETE OUTPUT FOLDER
                    self.output_folder_cleanup()

                    i = 0

    def _generate_metadata(self, table: TableDefinition):
        expected_columns = ['slug', 'shop_id']
        # validate
        self._validate_expected_columns('metadata', table, expected_columns)

        with open(table.full_path, 'r') as inp:
            reader = csv.DictReader(inp)
            i = 0
            for row in reader:
                out_file = self.create_out_file_definition(f'{row["shop_id"]}/{row["slug"]}/meta.json')
                # remove columns:
                for c in expected_columns:
                    row.pop(c, None)

                content = self._generate_metadata_content(list(row.keys()), list(row.values()))
                self._write_json_content_to_file(out_file, content[0])
                i += 1
                if i == self.chunksize:
                    logging.info(f"Uploading chunk for table {table.name} to S3")
                    # CREATE LIST OF FILES IN OUTPUT FOLDER
                    self.local_paths, self.target_paths = self.upload_processor.prepare_lists_of_files(
                        self.files_out_path,
                        self.s3_bucket_dir)
                    # SEND FILES TO TARGET DIR IN S3
                    self.upload_processor.process_upload(self.local_paths, self.target_paths)

                    # DELETE OUTPUT FOLDER
                    self.output_folder_cleanup()

                    i = 0

    @staticmethod
    def _generate_metadata_content(columns, row: List[str]):
        converter = Csv2JsonConverter(headers=columns, delimiter='__')
        return converter.convert_row(row, coltypes=coltypes_metadata, delimit="__", infer_undefined=True)



"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
