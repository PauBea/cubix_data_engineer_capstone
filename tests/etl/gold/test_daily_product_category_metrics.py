from datetime import datetime
from decimal import Decimal

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.gold.daily_product_category_metrics import get_daily_product_category_metrics


def test_get_daily_product_category_metrics(spark):
    """Positive test that the function get_daily_product_category_metrics returns the expected DataFrame.
    """

    wide_product_cat_test_data = [
        ("ProdCat-B", datetime(2017, 1, 1), Decimal("20.00"), Decimal("10.00")),
        ("ProdCat-B", datetime(2017, 1, 1), Decimal("40.00"), Decimal("20.00")),
        ("ProdCat-B", datetime(2017, 1, 10), Decimal("40.00"), Decimal("30.00")),
        ("ProdCat-B", datetime(2017, 1, 10), Decimal("60.00"), Decimal("40.00")),
        ("ProdCat-C", datetime(2017, 1, 1), Decimal("40.00"), Decimal("20.00")),
        ("ProdCat-C", datetime(2017, 1, 1), Decimal("80.00"), Decimal("40.00")),
        ("ProdCat-C", datetime(2017, 1, 10), Decimal("80.00"), Decimal("60.00")),
        ("ProdCat-C", datetime(2017, 1, 10), Decimal("120.00"), Decimal("80.00"))
    ]

    wide_product_cat_test_schema = st.StructType([
        st.StructField("EnglishProductCategoryName", st.StringType(), True),
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("SalesAmount", st.DecimalType(10, 2), True),
        st.StructField("Profit", st.DecimalType(10, 2), True)
    ])

    wide_product_cat_test = spark.createDataFrame(wide_product_cat_test_data, schema=wide_product_cat_test_schema)

    result = get_daily_product_category_metrics(wide_product_cat_test)

    expected_schema = st.StructType([
        st.StructField("EnglishProductCategoryName", st.StringType(), True),
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("SalesAmountSum", st.DecimalType(10, 2), True),
        st.StructField("SalesAmountAvg", st.DecimalType(10, 2), True),
        st.StructField("ProfitSum", st.DecimalType(10, 2), True),
        st.StructField("ProfitAvg", st.DecimalType(10, 2), True)
    ])

    expected_data = [
        (
            "ProdCat-B",
            datetime(2017, 1, 1),
            Decimal("60.00"),
            Decimal("30.00"),
            Decimal("30.00"),
            Decimal("15.00")
        ),
        (
            "ProdCat-B",
            datetime(2017, 1, 10),
            Decimal("100.00"),
            Decimal("50.00"),
            Decimal("70.00"),
            Decimal("35.00")
        ),
        (
            "ProdCat-C",
            datetime(2017, 1, 1),
            Decimal("120.00"),
            Decimal("60.00"),
            Decimal("60.00"),
            Decimal("30.00")
        ),
        (
            "ProdCat-C",
            datetime(2017, 1, 10),
            Decimal("200.00"),
            Decimal("100.00"),
            Decimal("140.00"),
            Decimal("70.00")
        )
    ]

    expected = spark.createDataFrame(expected_data,  schema=expected_schema)

    spark_testing.assertDataFrameEqual(result, expected)
