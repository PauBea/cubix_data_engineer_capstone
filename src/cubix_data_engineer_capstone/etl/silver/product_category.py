import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


PRODUCT_SUBCATEGORY_MAPPING = {
    "psk": "ProductSubCategoryKey",
    "pck": "ProductCategoryKey",
    "epsn": "EnglishProductSubcategoryName",
    "spsn": "SpanishProductSubcategoryName",
    "fpsn": "FrenchProductSubcategoryName"
}


def get_product_subcategory(product_subcategory_raw: DataFrame) -> DataFrame:
    """Map and select ProductSubcategory data.
    1. Select needed columns, and cast data types.
    2. Rename columns according to mapping.
    3. Drop duplicates.

    """
    return (
        product_subcategory_raw
        .select(
            sf.col("psk").cast("int"),
            sf.col("pck").cast("int"),
            sf.col("epsn"),
            sf.col("spsn"),
            sf.col("fpsn")
         )
        .withColumnsRenamed(PRODUCT_SUBCATEGORY_MAPPING)
        .dropDuplicates()
    )
