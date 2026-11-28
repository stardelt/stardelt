import pytest

from stardelt_pipelines.catalog_properties import catalog_properties, spark_catalog_configuration

BASE_ENV = {
    "CATALOG_URI": "https://lakekeeper.test/catalog",
    "S3_ENDPOINT": "https://s3.test",
    "S3_ACCESS_KEY": "access-key",
    "S3_SECRET_KEY": "s3-secret",
}

BASE_PROPERTIES = {
    "uri": "https://lakekeeper.test/catalog",
    "warehouse": "warehouse",
    "s3.endpoint": "https://s3.test",
    "s3.access-key-id": "access-key",
    "s3.secret-access-key": "s3-secret",
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
}

CREDENTIALS = {
    "CATALOG_CLIENT_ID": "svc-catalog-airflow",
    "CATALOG_CLIENT_SECRET": "client-secret",
    "CATALOG_TOKEN_URL": "https://id.test/realms/stardelt/protocol/openid-connect/token",
}


def test_the_complete_trio_signs_in_and_keeps_the_base_properties():
    properties = catalog_properties(BASE_ENV | CREDENTIALS)
    assert properties == BASE_PROPERTIES | {
        "credential": "svc-catalog-airflow:client-secret",
        "oauth2-server-uri": CREDENTIALS["CATALOG_TOKEN_URL"],
        "scope": "openid",
    }


def test_without_the_trio_the_properties_are_exactly_todays():
    assert catalog_properties(BASE_ENV) == BASE_PROPERTIES


@pytest.mark.parametrize(
    "partial",
    [{"CATALOG_TOKEN_URL": CREDENTIALS["CATALOG_TOKEN_URL"]}, {"CATALOG_CLIENT_ID": "svc-catalog-airflow"}],
    ids=["token-url-only", "client-id-only"],
)
def test_a_partial_trio_is_treated_as_absent(partial):
    assert catalog_properties(BASE_ENV | partial) == BASE_PROPERTIES


def test_spark_catalog_configuration_builds_a_block_per_catalog():
    configuration = spark_catalog_configuration(
        ("bronze", "silver"), "http://lakekeeper.test/catalog", "http://s3.test"
    )
    assert configuration["spark.sql.catalog.bronze"] == (
        "org.apache.iceberg.spark.SparkCatalog"
    )
    assert configuration["spark.sql.catalog.bronze.type"] == "rest"
    assert configuration["spark.sql.catalog.bronze.uri"] == "http://lakekeeper.test/catalog"
    assert configuration["spark.sql.catalog.bronze.warehouse"] == "bronze"
    assert configuration["spark.sql.catalog.silver.warehouse"] == "silver"
    assert not any("access-key" in key for key in configuration)
    assert not any(key.endswith(".token") for key in configuration)
