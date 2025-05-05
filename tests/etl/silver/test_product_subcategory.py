import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.product_subcategory import get_product_subcategory  # noqa E501


def test_get_product_subcategory(spark):
    """Positive test that the function get_product_subcategory
    returns the expected DataFrame.


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

    result = get_product_subcategory(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductSubCategoryKey", st.IntegerType(), True),  # noqa E501
            st.StructField("ProductCategoryKey", st.IntegerType(), True),  # noqa E501
            st.StructField("EnglishProductSubcategoryName", st.StringType(), True),  # noqa E501
            st.StructField("SpanishProductSubcategoryName", st.StringType(), True),  # noqa E501
            st.StructField("FrenchProductSubcategoryName", st.StringType(), True)  # noqa E501
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
