import os

from pyspark.sql import SparkSession


def _create_authentication_config(tenant_id: str, client_id: str, client_secret: str) -> dict[str, str]:   # noqa: E501
    """_summary_

    :param tenant_id: Tenant_ID of the app registration
    :param client_id: Client_ID of the app registration
    :param client_secret: Client_Secret of the app registration
    :return:
    """
    return {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",  # noqa: E501
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"  # noqa: E501
     }


def authenticate() -> None:
    """Authenticate the SparkSession using
    the predefined environment variables.
    """
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    if not tenant_id or not client_id or not client_secret:
        raise ValueError("Missing one or more required environment variables: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET")  # noqa: E501

    spark = SparkSession.getActiveSession()

    config = _create_authentication_config(tenant_id, client_id, client_secret)

    for key, value in config.items():
        spark.conf.set(key, value)
