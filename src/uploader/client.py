import logging
import boto3
import os

# configuration variables
KEY_FORMAT = 'format'
AWS_SECRET_ACCESS_KEY = '#aws_secret_access_key'
AWS_ACCESS_KEY_ID = 'aws_access_key_id'
WORKERS = "workers"
S3_BUCKET_DIR = "aws_directory"


class S3Writer:
    """
    This class handles the logic to upload files to AWS S3.
    """

    def __init__(self, params, data_path, aws_bucket):
        super().__init__()
        self.aws_bucket = aws_bucket
        self.data_path = data_path
        self.client = self.get_client_from_session(params)
        self.sent_files_counter = 0

    def process_upload(self, local_paths, target_paths):
        """
        Uploads files to S3 storage.

        Returns: None
        """
        for file_to_upload, target_path in zip(local_paths, target_paths):
            self.upload_one_file(self.aws_bucket, self.client, file_to_upload, target_path)

    @staticmethod
    def get_client_from_session(params) -> boto3.Session.client:
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
        return session.client('s3')

    def test_connection_ok(self, params) -> bool:
        try:
            self.client.head_bucket(Bucket=self.aws_bucket)
            logging.info("S3 Connection successful.")
            return True
        except Exception as e:
            logging.warning(e)
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
        self.sent_files_counter += 1
