import os
import sys
import logging
import argparse
import boto3


class AwsConfigException(Exception):
    pass


class AwsClientService(object):
    """
    AwsClientService provides a boto3 client for the needed AWS service and configures the client
    either with AWS credentials from the environment or via AWS credentials profile.
    """

    def __init__(self, aws_profile=None):
        self.aws_profile = aws_profile

    def get_aws_client(self, aws_service: str):
        if self.aws_profile:
            boto3.setup_default_session(profile_name=self.aws_profile)
            return boto3.client(aws_service)
        else:
            aws_credentials = self._get_aws_credentials()
            return boto3.client(
                aws_service,
                aws_access_key_id=aws_credentials['aws_access_key_id'],
                aws_secret_access_key=aws_credentials['aws_secret_access_key'],
                region_name=aws_credentials['aws_region_name']
            )

    @staticmethod
    def _get_aws_credentials():
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


class AwsCognitoUser(object):

    AWS_SERVICE = 'cognito-idp'

    def __init__(self, user_pool_id, aws_profile=None, verbose=False):
        self.verbose = verbose
        self.log = self._get_logger()
        self.aws_cognito_client = AwsClientService(aws_profile=aws_profile).get_aws_client(aws_service=self.AWS_SERVICE)
        self.user_pool_id = user_pool_id

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

    def get_user_details(self, username):
        """
        Gets informations about the user from aws cognito user pool
        :return: aws cognito response object
        """
        self._log(f'Get user informations from cognito for user <{username}>')
        users = self.aws_cognito_client.list_users(
            UserPoolId=self.user_pool_id,
            Filter=f'username = \"{username}\"'
        )
        return users


def main(sys_args):
    def print_user(user_details):
        from pprint import PrettyPrinter
        pp = PrettyPrinter(indent=4)
        pp.pprint(user_details)

    parser = argparse.ArgumentParser(sys_args)
    parser.add_argument('-u', '--username', type=str, help='Username of the AWS cognito user', required=True)
    parser.add_argument('-c', '--cognito-userpool', type=str, help='ID of the AWS cognito user pool', required=True)
    parser.add_argument('-p', '--profile', type=str, help='AWS profile to use')
    parser.add_argument('-v', '--verbose', action="store_true", help='Extended log informations')
    args = parser.parse_args()

    aws_cognito_user_client = AwsCognitoUser(user_pool_id=args.cognito_userpool, aws_profile=args.profile,
                                             verbose=args.verbose)
    user = aws_cognito_user_client.get_user_details(username=args.username)
    print_user(user)


if __name__ == '__main__':
    main(sys.argv[1:])
