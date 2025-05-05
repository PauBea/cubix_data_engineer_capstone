import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.products_subcategory import get_products_subcategory  # noqa E501


def test_get_products_subcategory(spark):
    """Positive test that the function get_sales
    returns the expected DataFrame.

    :param spark: _description_
    """
    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("1", "1", "epsn_1", "spsn_1", "fpsn_1", "extra_value"),  # noqa E501
            # exclude - duplicate
            ("1", "1", "epsn_1", "spsn_1", "fpsn_1", "extra_value"),  # noqa E501
        ],
        schema=[
                "psk",
                "pck",
                "epsn",
                "spsn",
                "fpsn"
            ]

    )

    result = get_products_subcategory(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductSubcategoryName", st.StringType(), True),
            st.StructField("SpanishProductSubcategoryName", st.StringType(), True),
            st.StructField("FrenchProductSubcategoryName", st.StringType(), True)
        ]
    )

    expected = spark.createDataFrame(
        [
            (
               1,
               1,
               "epsn_1",
               "spsn_1",
               "fpsn_1"
            )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)
