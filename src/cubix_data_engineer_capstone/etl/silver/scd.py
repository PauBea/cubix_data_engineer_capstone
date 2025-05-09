from delta.tables import DeltaTable

from pyspark.sql import DataFrame, SparkSession

from cubix_data_engineer_capstone.utils.config import STORAGE_ACCOUNT_NAME


def scd1(spark: SparkSession, container_name: str, file_path: str, new_data: DataFrame, primary_key: str):

    master_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"
    delta_master = DeltaTable.forPath(spark, master_path)

    (
        delta_master
        .alias("master")
        .merge(
            new_data.alias("updates"),
            f"master.{primary_key} = updates.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
