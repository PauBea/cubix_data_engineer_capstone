import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


PRODUCT_CATEGORY_MAPPING = {
    "pck": "ProductCategoryKey",
    "epcn": "EnglishProductCategoryName",
    "spcn": "SpanishProductCategoryName",
    "fpcn": "FrenchProductCategoryName"
}


def get_product_category(product_category_raw: DataFrame) -> DataFrame:
    """Map and select ProductCategory data.
    1. Select needed columns and cast data types.
    2. Rename columns according to mapping.
    3. Drop duplicates.

    """
    return (
        product_category_raw
        .select(
            sf.col("pck").cast("int"),
            sf.col("epcn"),
            sf.col("spcn"),
            sf.col("fpcn")
         )
        .withColumnsRenamed(PRODUCT_CATEGORY_MAPPING)
        .dropDuplicates()
    )
