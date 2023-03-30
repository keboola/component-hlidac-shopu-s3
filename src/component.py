"""
Template Component main class.

"""
import csv
import json
import logging
from typing import List
import os
import shutil
import io
import zipfile

from csv2json.hone_csv2json import Csv2JsonConverter
from keboola.component.base import ComponentBase
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from uploader.client import S3Writer

# configuration variables
KEY_FORMAT = 'format'
KEY_OVERRIDE = 'override_default_values'
AWS_SECRET_ACCESS_KEY = '#aws_secret_access_key'
AWS_ACCESS_KEY_ID = 'aws_access_key_id'
AWS_BUCKET = "aws_bucket"
S3_BUCKET_DIR = "aws_directory"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [AWS_SECRET_ACCESS_KEY,
                       AWS_ACCESS_KEY_ID,
                       AWS_BUCKET,
                       KEY_FORMAT]


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters
        self.upload_processor = None
        self.target_paths = None
        self.local_paths = None

        if params.get(KEY_OVERRIDE, False):
            self.s3_bucket_dir = params.get(S3_BUCKET_DIR)
            self.aws_bucket = params.get(AWS_BUCKET)
            logging.info(f"Component will use overriden values from config: s3_bucket_dir: {self.s3_bucket_dir}, "
                         f"aws_bucket: {self.aws_bucket}")
        else:
            self.s3_bucket_dir = ""
            self.aws_bucket = "ingest.hlidacshopu.cz"
            logging.info(f"Component will use default values for config: s3_bucket_dir: {self.s3_bucket_dir}, "
                         f"aws_bucket: {self.aws_bucket}")

        # Access parameters in data/config_pricehistory.json
        if params.get(KEY_FORMAT):
            logging.info(f"Format setting is: {params.get(KEY_FORMAT)}")

        self.custom_mapping = [] if params.get("field_datatypes") is None else params.get("field_datatypes")

    def run(self):
        """
        Main execution code
        """

        input_tables = self.get_input_tables_definitions()

        self.upload_processor = S3Writer(self.configuration.parameters, self.files_out_path,
                                         aws_bucket=self.aws_bucket)

        if not self.upload_processor.test_connection_ok():
            logging.error("Connection check failed. Connection is not possible or your account does not have "
                          "READ_ACP rights.")

        for table in input_tables:
            _format = self.configuration.parameters[KEY_FORMAT]
            logging.info(f"Processing table {table.name} using format: {_format}")
            if _format == 'pricehistory':
                self._generate_price_history(table)
            elif _format == 'metadata':
                self._generate_metadata(table)
            else:
                raise UserException(f"Wrong parameter in data/config_pricehistory.json {_format}. "
                                    "Viable parameters are: pricehistory/metadata")

        # self.output_folder_cleanup()
        logging.info(f"Parsing finished successfully. "
                     f"Component processed {self.upload_processor.sent_files_counter} files.")

    @staticmethod
    def _validate_expected_columns(table_type, table: TableDefinition, expected_columns: List[str]):
        errors = []
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

    @staticmethod
    def _write_json_content_to_zip(zip_file, file_path, content):
        with io.BytesIO() as file_buffer:
            # Write JSON content to the in-memory file buffer
            json_str = json.dumps(content, ensure_ascii=False)
            file_buffer.write(json_str.encode('utf-8'))
            file_buffer.seek(0)

            # Add the in-memory file to the zip file
            zip_file.writestr(file_path, file_buffer.getvalue())

    def _generate_price_history(self, table: TableDefinition):
        expected_columns = ['shop_id', 'slug', 'json']
        self._validate_expected_columns('pricehistory', table, expected_columns)

        # Create a dictionary to store the zip files by shop_id
        zip_files = {}

        logging.info("Writing json content.")
        for row in self.read_csv_file(table.full_path):
            shop_id = row["shop_id"]

            if shop_id not in zip_files:
                # Create the zip file with the desired filename
                suffix = "_pricehistory"
                zip_filename = os.path.join(self.files_out_path, f'{shop_id}{suffix}.zip')
                zip_files[shop_id] = zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED)

            # Remove the top-level folder by excluding the `{row["shop_id"]}` part
            file_path = f'items/{row["shop_id"]}/{row["slug"]}/price-history.json'
            content = json.loads(row['json'])
            self._write_json_content_to_zip(zip_files[shop_id], file_path, content)

        # Close the zip files
        for zip_file in zip_files.values():
            zip_file.close()

        logging.info("Uploading files.")
        self._send_data(table)

    def _generate_metadata(self, table: TableDefinition):
        expected_columns = ['slug', 'shop_id']
        self._validate_expected_columns('metadata', table, expected_columns)
        with open(table.full_path, 'r') as inp:
            reader = csv.DictReader(inp)
            for row in reader:
                out_file = self.create_out_file_definition(f'{row["shop_id"]}/items/{row["shop_id"]}/{row["slug"]}'
                                                           f'/meta.json')
                for c in expected_columns:
                    row.pop(c, None)
                content = self._generate_metadata_content(list(row.keys()), list(row.values()))
                self._write_json_content_to_file(out_file, content[0])
            self.zip_and_clean_folders(self.files_out_path, "metadata")
        self._send_data(table)

    def _generate_metadata_content(self, columns, row: List[str]):
        converter = Csv2JsonConverter(headers=columns, delimiter='__')
        return converter.convert_row(row, coltypes=self.custom_mapping, delimit="__", infer_undefined=True)

    def _send_data(self, table):
        """
        Sends data to S3 and cleans the output folder.
        """
        logging.info(f"Uploading data for table {table.name} to S3")
        # CREATE LIST OF FILES IN OUTPUT FOLDER
        self.local_paths, self.target_paths = self.upload_processor.prepare_lists_of_files(
            self.files_out_path,
            self.s3_bucket_dir)
        # SEND FILES TO TARGET DIR IN S3
        self.upload_processor.process_upload(self.local_paths, self.target_paths)

    @staticmethod
    def read_csv_file(file_path):
        with open(file_path, 'r', encoding='utf-8') as inp:
            reader = csv.DictReader(inp)
            for row in reader:
                yield row


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
