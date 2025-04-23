from pyspark.sql import DataFrame, SparkSession


from cubix_data_engineer_capstone.utils.config import STORAGE_ACCOUNT_NAME


def read_file_from_datalake(container_name: str, file_path: str, format: str) -> DataFrame:
    """Reads a file from Azure DataLake and returns it as a Spark Dataframe

    Args:
        container_name (str):   The name of the file system (container) in Azure.
        file_path (str):        The path to the file in the data lake.
        format (str):           The format if the file("csv", "json", "delta", "parquet").
        return:                 DataFrame with a loaded data.
    """
    if format not in ["csv", "parquet", "delta", "json"]:
        raise ValueError(f"Invalid format: {format}. Supported formats are: csv, json, parquet, delta")

    file_path = f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{file_path}"

    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active SparkSession found")
 
    if format == "json":
        df = spark.read.json(file_path)
        return df
    else:
        df = (
            spark
            .read
            .format(format)
            .option("header", "true")
            .load(file_path, format=format)
        )

    return df
