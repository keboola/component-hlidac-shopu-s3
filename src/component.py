"""
Template Component main class.

"""
import csv
import json
import logging
from pathlib import Path
from typing import List
from tqdm import tqdm
import boto3
import botocore
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

from csv2json.hone_csv2json import Csv2JsonConverter
from keboola.component.base import ComponentBase
from keboola.component.dao import TableDefinition, FileDefinition
from keboola.component.exceptions import UserException

# configuration variables
KEY_FORMAT = 'format'
AWS_SECRET_ACCESS_KEY = '#aws_secret_access_key'
AWS_ACCESS_KEY_ID = 'aws_access_key_id'
AWS_BUCKET = "aws_bucket"
WORKERS = "workers"
S3_BUCKET_DIR = "s3_test/"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [AWS_SECRET_ACCESS_KEY,
                       AWS_ACCESS_KEY_ID,
                       AWS_BUCKET,
                       KEY_FORMAT]


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
        self.client = None
        self.failed_uploads = []
        self.exceptions = []
        self.target_paths = None
        self.local_paths = None
        self.workers = 1

    def run(self):
        """
        Main execution code
        """
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Access parameters in data/config.json
        if params.get(KEY_FORMAT):
            logging.info(f"Format setting is: {params.get(KEY_FORMAT)}")

        if params.get(WORKERS):
            self.workers = int(params.get(WORKERS))
            logging.info(f"Number of workers set: {self.workers}")
        else:
            logging.warning("Number of workers is not set. Using serial mode.")

        self.client = self.get_client_from_session(params)

        if not self.test_connection_ok(params):
            raise ConnectionError("Connection failed")

        input_tables = self.get_input_tables_definitions()

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

        # TODO is there a better way to get data/out/files folder?
        data_path = self.files_out_path
        self.local_paths, self.target_paths = self.prepare_lists_of_files(data_path, S3_BUCKET_DIR)

        self.process_upload()

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
            for row in reader:
                out_file = self.create_out_file_definition(f'{row["shop_id"]}/{row["slug"]}/price-history.json')
                content = json.loads(row['json'])
                self._write_json_content_to_file(out_file, content)

    def _generate_metadata_content(self, columns, row: List[str]):
        converter = Csv2JsonConverter(headers=columns, delimiter='__')
        return converter.convert_row(row, [], '__', infer_undefined=True)

    def _generate_metadata(self, table: TableDefinition):
        expected_columns = ['slug', 'shop_id']
        # validate
        self._validate_expected_columns('metadata', table, expected_columns)

        with open(table.full_path, 'r') as inp:
            reader = csv.DictReader(inp)
            for row in reader:
                out_file = self.create_out_file_definition(f'{row["shop_id"]}/{row["slug"]}/meta.json')
                # remove columns:
                for c in expected_columns:
                    row.pop(c, None)

                content = self._generate_metadata_content(list(row.keys()), list(row.values()))
                self._write_json_content_to_file(out_file, content[0])

    def process_upload(self):
        """
        inspired by https://emasquil.github.io/posts/multithreading-boto3/

        Uploads file to S3 storage in threads with number of workers defined by WORKERS parameter.

        Returns: None
        """
        logging.info(f"Processing {len(self.local_paths)} files.")

        func = partial(self.upload_one_file, AWS_BUCKET, self.client)

        with tqdm(desc="Uploading files to S3", total=len(self.local_paths)) as pbar:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {
                    executor.submit(func, file_to_upload, target_path): [file_to_upload, target_path] for
                    file_to_upload, target_path in zip(self.local_paths, self.target_paths)
                }
                for future in as_completed(futures):
                    if future.exception():
                        self.failed_uploads.append(futures[future])
                        self.exceptions.append(future.exception())
                    pbar.update(1)
        if len(self.failed_uploads) > 0:
            logging.info("Some uploads have failed.")
            # TODO implement proper error log
            """
            with open(
                    os.path.join(INPUT_DIR, "failed_uploads.csv"), "w", newline=""
            ) as csvfile:
                wr = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                wr.writerow(failed_uploads)
            """
        else:
            logging.info('All files were successfully sent!')

    def get_client_from_session(self, params) -> boto3.Session.client:
        """
        Creates and returns boto3 client class.

        Args:
            params: Keboola json configuration parameters

        Returns:
            boto3 client class

        """
        session = boto3.Session(
            aws_access_key_id=params.get(AWS_ACCESS_KEY_ID),
            aws_secret_access_key=params.get(AWS_SECRET_ACCESS_KEY)
        )
        return session.client('s3', config=botocore.client.Config(max_pool_connections=self.workers + 8))

    def test_connection_ok(self, params) -> bool:
        conn_test = self.client.get_bucket_acl(Bucket=params.get(AWS_BUCKET))
        if conn_test["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logging.info("S3 Connection successful.")
            return True
        return False

    @staticmethod
    def prepare_lists_of_files(in_dir, out_dir):
        """
        Creates and populates lists with local paths to files and target paths

        Returns:
            Two lists representing files that will be sent and their destination

        Args:
            in_dir: input directory
            out_dir: target S3 folder
        """

        _local_paths, _target_paths = [], []
        for root, dirs, files in os.walk(in_dir):
            for name in files:
                _local_paths.append(os.path.join(root, name))
                _target_paths.append(out_dir + os.path.join(root, name).replace(in_dir, "")[1:])

        return _local_paths, _target_paths

    @staticmethod
    def upload_one_file(bucket: str, client: boto3.client, local_file: str, target_path: str) -> None:
        """
        Download a single file from S3
        Args:
            bucket (str): S3 bucket where images are hosted
            target_path (str): S3 dir to store the file to
            client (boto3.client): S3 client
            local_file (str): S3 file name
        """

        client.upload_file(
            local_file, bucket, target_path
        )


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
