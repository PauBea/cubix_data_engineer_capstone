import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.product_category import get_product_category  # noqa E501


def test_get_product_category(spark):
    """Positive test that the function get_product_category
    returns the expected DataFrame.

    """

    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("1", "epcn_1", "spcn_1", "fpcn_1", "extra_value"),
            # exclude - duplicate
            ("1", "epcn_1", "spcn_1", "fpcn_1", "extra_value"),
        ],
        schema=[
                "pck",
                "epcn",
                "spcn",
                "fpcn"
            ]

    )

    result = get_product_category(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductCategoryKey", st.IntegerType(), True),  # noqa E501
            st.StructField("EnglishProductCategoryName", st.StringType(), True),  # noqa E501
            st.StructField("SpanishProductCategoryName", st.StringType(), True),  # noqa E501
            st.StructField("FrenchProductCategoryName", st.StringType(), True)  # noqa E501
        ]
    )

    expected = spark.createDataFrame(
        [
            (
               1,
               "epcn_1",
               "spcn_1",
               "fpcn_1"
            )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)
