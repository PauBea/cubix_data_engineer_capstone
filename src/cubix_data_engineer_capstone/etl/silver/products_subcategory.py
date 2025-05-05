import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


PRODUCTS_SUBCATEGORY_MAPPING = {
    "psk": "ProductSubCategoryKey",
    "pck": "ProductCategoryKey",
    "epsn": "EnglishProductSubcategoryName",
    "spsn": "SpanishProductSubcategoryName",
    "fpsn": "FrenchProductSubcategoryName"
}


def get_products_subcategory(products_subcategory_raw: DataFrame) -> DataFrame:
    """Map and select Sales data.

    :param sales_raw: Raw Sales data
    :return: Mapped and filtered Sales data.
    """
    return (
        products_subcategory_raw
        .select(
            sf.col("psk").cast("int"),
            sf.col("pck").cast("int"),
            sf.col("epsn"),
            sf.col("spsn"),
            sf.col("fpsn")
         )
        .withColumnsRenamed(PRODUCTS_SUBCATEGORY_MAPPING)
        .dropDuplicates()
    )
