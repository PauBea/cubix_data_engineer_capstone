import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


SALES_MAPPING = {
    "son": "SalesOrderNumber",
    "orderdate": "OrderDate",
    "pk": "ProductKey",
    "ck": "CustomerKey",
    "dateofshipping": "ShipDate",
    "oquantity": "OrderQuantity"
}


def get_sales(sales_raw: DataFrame) -> DataFrame:
    """Map and select Sales data.

    :param sales_raw: Raw Sales data
    :return: Mapped and filtered Sales data.
    """
    return (
        sales_raw
        .select(
            sf.col("son").cast("date"),
            sf.col("orderdate"),
            sf.col("pk").cast("int"),
            sf.col("ck").cast("int"),
            sf.col("dateofshipping").cast("date"),
            sf.col("oquantity").cast("int"),
        )
        .withColumnRenamed(SALES_MAPPING)
        .dropDuplicates()
    )
