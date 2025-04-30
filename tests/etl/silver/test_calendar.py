
from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_data_engineer_capstone.etl.silver.calendar import get_calendar


def test_get_calendar(spark):
    """Positive test that the function get_calendar
    returns the expected DataFrame.

    :param spark: _description_
    """
    test_data = spark.createDataFrame(
        [
            ("2017-01-03", "2", "Tuesday", "January", "1", "3", "1", "1", "2017", "2016", "1", "1", "7", "3", "201701", "extra_value"),  # noqa: E501
            ("2017-01-03", "2", "Tuesday", "January", "1", "3", "1", "1", "2017", "2016", "1", "1", "7", "3", "201701", "extra_value")   # noqa: E501
        ],
        schema=[
            "Date",
            "DayNumberOfWeek",
            "DayName",
            "MonthName",
            "MonthNumberOfYear",
            "DayNumberOfYear",
            "WeekNumberOfYear",
            "CalendarQuarter",
            "CalendarYear",
            "FiscalYear",
            "FiscalSemester",
            "FiscalQuarter",
            "FinMonthNumberOfYear",
            "DayNumberOfMonth",
            "MonthID",
            "extra_col"

        ]

    )

    result = get_calendar(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("Date", st.DateType(), True),
            st.StructField("DayNumberOfWeek", st.IntegerType(), True),
            st.StructField("DayName", st.StringType(), True),
            st.StructField("MonthName", st.StringType(), True),
            st.StructField("MonthNumberOfYear", st.IntegerType(), True),
            st.StructField("DayNumberOfYear", st.IntegerType(), True),
            st.StructField("WeekNumberOfYear", st.IntegerType(), True),
            st.StructField("CalendarQuarter", st.IntegerType(), True),
            st.StructField("CalendarYear", st.IntegerType(), True),
            st.StructField("FiscalYear", st.IntegerType(), True),
            st.StructField("FiscalSemester", st.IntegerType(), True),
            st.StructField("FiscalQuarter", st.IntegerType(), True),
            st.StructField("FinMonthNumberOfYear", st.IntegerType(), True),
            st.StructField("DayNumberOfMonth", st.IntegerType(), True),
            st.StructField("MonthID", st.IntegerType(), True)

        ]
    )

    expected = spark.createDataFrame(

        [
            (
                datetime(2017, 1, 3),
                2,
                "Tuesday",
                "January",
                1,
                3,
                1,
                1,
                2017,
                2016,
                1,
                1,
                7,
                3,
                201701
            )

        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)
