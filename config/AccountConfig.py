import logging

class AwsConfig:

    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.INFO)

    __vars = {        
        "accountID": "187879053795",
        "region":"us-east-1"
    }

    # @staticmethod
    # def init():
    #     LOGGER.info("Configuring app settings")

    @staticmethod
    def account_id() -> str:
        return AwsConfig.__vars['accountID']

    @staticmethod
    def region() -> str:
        return AwsConfig.__vars['region']