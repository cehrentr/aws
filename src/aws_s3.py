import os
import sys
import logging
import argparse
import boto3


class AwsConfigException(Exception):
    pass


class AwsS3Download(object):

    AWS_SERVICE = 's3'
    CURRENT_WORKING_DIR = os.getcwd()

    def __init__(self, aws_profile=None, verbose=False):
        self.verbose = verbose
        self.log = self._get_logger()
        self.aws_s3_client = self._get_s3_client(aws_profile)

    def _log(self, message):
        """
        Logs incoming messages
        :param message: Message to log
        """
        if self.verbose:
            self.log.info(message)

    def _get_logger(self):
        """
        Returns a logging instance
        :return: logging instance
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        return logging.getLogger(self.__class__.__name__)

    def _get_s3_client(self, aws_profile):
        """
        Returns a boto3 resource instance either configured with AWS credentials from
        environment or via aws credentials profile
        :param aws_profile: aws profile to use
        :return: AWS boto3 resource instance
        """
        if aws_profile:
            self._log(f'Found profile <{aws_profile}> and set up session.')
            boto3.setup_default_session(profile_name=aws_profile)
            return boto3.resource(self.AWS_SERVICE)
        else:
            self._log(f'No profile provided. Continue with environment AWS credentials.')
            aws_credentials = self._get_aws_credentials()
            return boto3.resource(
                self.AWS_SERVICE,
                aws_access_key_id=aws_credentials['aws_access_key_id'],
                aws_secret_access_key=aws_credentials['aws_secret_access_key'],
                region_name=aws_credentials['aws_region_name']
            )

    def _get_aws_credentials(self):
        """
        Gets AWS credentials from the environment
        :return: AWS credentials as dictionary
        """
        self._log('Looking for environment AWS credentials.')

        aws_region_name = os.getenv('AWS_DEFAULT_REGION', os.getenv('AWS_REGION', ''))
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID_DEV', '')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY_DEV', '')

        if not aws_region_name:
            raise AwsConfigException('Environment variable [AWS_DEFAULT_REGION | AWS_REGION] not set')
        if not aws_access_key_id:
            raise AwsConfigException('Environment variable AWS_ACCESS_KEY_ID_DEV not set')
        if not aws_secret_access_key:
            raise AwsConfigException('Environment variable AWS_SECRET_ACCESS_KEY_DEV not set')

        return {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'aws_region_name': aws_region_name
        }

    def get_s3_bucket_list(self, s3_bucket: str, s3_bucket_key=''):
        """
        List either all items of the s3 bucket or only a subset which depends on
        the value of s3_bucket_key.
        :param s3_bucket: AWS s3 bucket
        :param s3_bucket_key: AWS s3 bucket key as a filter
        :return: AWS s3 bucket size and items as dictionary
        """
        bucket = self.aws_s3_client.Bucket(s3_bucket).objects.all()
        bucket_size = sum(1 for item in bucket if item.key.startswith(s3_bucket_key))
        self._log(f'Bucket <{s3_bucket}> contains {bucket_size} item(s) with key <{s3_bucket_key}>.')

        bucket_items = []
        for item in bucket:
            if item.key.startswith(s3_bucket_key):
                bucket_items.append({
                    'bucket_name': item.bucket_name,
                    'bucket_key': item.key
                })

        return {
            'bucket_size': bucket_size,
            'bucket_items': bucket_items
        }

    def get_s3_bucket(self, s3_bucket: str, s3_bucket_key='', target_folder=None):
        """
        Gets either all items of the s3 bucket or only a subset which depends on
        the value of s3_bucket_key.
        :param s3_bucket: AWS s3 bucket
        :param s3_bucket_key: AWS s3 bucket key as a filter
        :param target_folder: Local target folder where to download the bucket items to
        """
        bucket = self.get_s3_bucket_list(s3_bucket=s3_bucket, s3_bucket_key=s3_bucket_key)
        if bucket['bucket_size'] > 0:

            target_folder = target_folder if target_folder else f'{self.CURRENT_WORKING_DIR}/{s3_bucket}'

            if not os.path.exists(target_folder):
                self._log(f'Target dir <{target_folder}> does not exist and will be created.')
                os.makedirs(target_folder)

            for item in bucket['bucket_items']:
                self._get_s3_bucket_item(item['bucket_name'], item['bucket_key'], target_folder)

    def _get_s3_bucket_item(self, s3_bucket: str, s3_bucket_key: str, target_folder: str):
        """
        Downloads the s3_bucket_key item from s3_bucket to the local target folder
        :param s3_bucket: AWS s3 bucket
        :param s3_bucket_key: AWS s3 bucket key as a filter
        :param target_folder: Local target folder where to download the bucket items to
        """
        target_file = '{0}/{1}'.format(target_folder, s3_bucket_key.replace('/', '_'))
        self._log(f'Download <{s3_bucket}/{s3_bucket_key}> to <{target_file}>')
        self.aws_s3_client.Bucket(s3_bucket).download_file(s3_bucket_key, target_file)


def main(sys_args):
    def print_bucket_content(_bucket_items):
        from pprint import PrettyPrinter
        pp = PrettyPrinter(indent=4)
        pp.pprint(_bucket_items)

    parser = argparse.ArgumentParser(sys_args)
    parser.add_argument('-b', '--bucket', type=str, help='Name of the AWS s3 bucket', required=True)
    parser.add_argument('-k', '--bucket-key', type=str, help='Name of the AWS s3 bucket key')
    parser.add_argument('-p', '--profile', type=str, help='AWS profile to use')
    parser.add_argument('-d', '--dryrun', action="store_true", help='Lists all bucket items.')
    parser.add_argument('-v', '--verbose', action="store_true", help='Extended log informations')
    args = parser.parse_args()

    aws_s3_downloader = AwsS3Download(aws_profile=args.profile, verbose=args.verbose)

    if args.dryrun:
        bucket_items = aws_s3_downloader.get_s3_bucket_list(s3_bucket=args.bucket, s3_bucket_key=args.bucket_key)
        print_bucket_content(bucket_items)
    else:
        aws_s3_downloader.get_s3_bucket(s3_bucket=args.bucket, s3_bucket_key=args.bucket_key)


if __name__ == '__main__':
    main(sys.argv[1:])
