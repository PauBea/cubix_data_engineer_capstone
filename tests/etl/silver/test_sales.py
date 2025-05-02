from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.sales import get_sales


def test_get_sales(spark):
    """Positive test that the function get_sales
    returns the expected DataFrame.

    :param spark: _description_
    """
    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("son_1", "2017-01-03", "1", "2", "2017-02-03", "3", "extra_value"),  # noqa E501
            # exclude - duplicate
            ("son_1", "2017-01-03", "1", "2", "2017-02-03", "3", "extra_value"),  # noqa E501
        ],
        schema=[
            "son",
            "orderdate",
            "pk",
            "ck",
            "dateofshipping",
            "oquantity",
            "extra_col"


        ]

    )

    result = get_sales(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("SalesOrderNumber", st.StringType(), True),
            st.StructField("OrderDate", st.DateType(), True),
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("CustomerKey", st.IntegerType(), True),
            st.StructField("ShipDate", st.DateType(), True),
            st.StructField("OrderQuantity", st.IntegerType(), True)

        ]
    )

    expected = spark.createDataFrame(
        [
            (
                "son_1",
                datetime(2017, 1, 3),
                1,
                2,
                datetime(2017, 2, 3),
                3
            )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)
