"""
Template Component main class.

"""

import logging
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
import boto3
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import botocore
import csv

# configuration variables
AWS_SECRET_ACCESS_KEY = '#aws_secret_access_key'
AWS_ACCESS_KEY_ID = 'aws_access_key_id'
AWS_BUCKET = "aws_bucket"
WORKERS = "workers"
OUTPUT_DIR = "s3_test/"
INPUT_DIR = "/Users/dominik/projects/kds-team.wr-hlidac-shopu-s3/data/out/files/"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [AWS_SECRET_ACCESS_KEY,
                       AWS_ACCESS_KEY_ID,
                       AWS_BUCKET]


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

        if params.get(WORKERS):
            self.workers = int(params.get(WORKERS))
            logging.info(f"Number of workers set: {self.workers}")
        else:
            logging.warning("Number of workers is not set. Using serial mode.")

        self.client = self.get_client_from_session(params)

        conn_test = self.client.get_bucket_acl(Bucket=params.get(AWS_BUCKET))
        if conn_test["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logging.info("Connection successful.")
        else:
            logging.error("Connection failed")

        self.local_paths, self.target_paths = self.prepare_lists_of_files(INPUT_DIR, OUTPUT_DIR)

        self.process_upload()

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
                # Using a dict for preserving the downloaded file for each future, to store it as a failure if we
                # need that
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

    def upload_one_file(self, bucket: str, client: boto3.client, local_file: str, target_path: str) -> None:
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
        cwd = os.getcwd()
        local_path = os.path.join(cwd.replace("src", ""), "data/out/files")
        for root, dirs, files in os.walk(in_dir):
            for name in files:
                _local_paths.append(os.path.join(root, name))
                _target_paths.append(out_dir + os.path.join(root, name).replace(local_path, "")[1:])

        return _local_paths, _target_paths


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
