
import pyspark.sql.types as st
import pyspark.testing as spark_testing
from decimal import Decimal
from cubix_data_engineer_capstone.etl.silver.products import get_products


def test_get_products(spark):
    """Positive test that the function get_products
    returns the expected DataFrame.

    :param spark: _description_
    """
    test_data = spark.createDataFrame(
        [
            # include - sample to keep
            ("1", "1", "product_1", "30.3333", "50.1111", "60.2222", "Blue", "50", "50-52cm", "2.24", "modelname_1", "100", "desc_1", "extra_value"),  # noqa E501
            # exclude - duplicate
            ("1", "1", "product_1", "30.3333", "50.1111", "60.2222", "Blue", "50", "50-52cm", "2.24", "modelname_1", "100", "desc_1", "extra_value"),  # noqa E501
            # incluce - Range = "NA"
            ("2", "2", "product_2", "30.3333", "50.1111", "60.2222", "Red", "100", "NA", "2.24", "modelname_2", "100", "desc_2", "extra_value")  # noqa E501
        ],
        schema=[
            "pk",
            "psck",
            "name",
            "stancost",
            "dealerprice",
            "listprice",
            "color",
            "size",
            "range",
            "weight",
            "nameofmodel",
            "ssl",
            "desc"
        ]

    )

    result = get_products(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
            st.StructField("ProductName", st.StringType(), True),
            st.StructField("StandardCost", st.DecimalType(10, 2), True),
            st.StructField("DealerPrice", st.DecimalType(10, 2), True),
            st.StructField("ListPrice", st.DecimalType(10, 2), True),
            st.StructField("Color", st.StringType(), True),
            st.StructField("Size", st.IntegerType(), True),
            st.StructField("SizeRange", st.StringType(), True),
            st.StructField("Weight", st.DecimalType(10, 2), True),
            st.StructField("ModelName", st.StringType(), True),
            st.StructField("SafetyStockLevel", st.IntegerType(), True),
            st.StructField("Description", st.StringType(), True),
            st.StructField("ProfitMargin", st.DecimalType(10, 2), True)
        ]
    )

    expected = spark.createDataFrame(
        [
            (
                1,
                1,
                "product_1",
                Decimal("30.33"),
                Decimal("50.11"),
                Decimal("60.22"),
                "Blue",
                50,
                "50-52cm",
                Decimal("2.24"),
                "modelname_1",
                100,
                "desc_1",
                Decimal("10.11")
             ),

            (
                2,
                2,
                "product_2",
                Decimal("30.33"),
                Decimal("50.11"),
                Decimal("60.22"),
                "Red",
                100,
                None,
                Decimal("2.24"),
                "modelname_2",
                100,
                "desc_2",
                Decimal("10.11")

             )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)
