import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def _join_master_tables(
        sales_master: DataFrame,
        calendar_master: DataFrame,
        customers_master: DataFrame,
        products_master: DataFrame,
        product_category_master: DataFrame,
        product_subcategory_master: DataFrame
) -> DataFrame:
    """Join the master DataFrames to te Sales Master.
    Drop Date, ProductSubCategoryKey and
    ProductCategoryKey to avoid duplicate columns.

    :param sales_master: Master DataFrames for Sales.
    :param calendar_master: Master DataFrames for Calendar.
    :param customers_master: Master DataFrames for Customers.
    :param products_master: Master DataFrames for Products.
    :param product_category_master:
        Master DataFrames for Products_Category.
    :param product_subcategory_master: Master DataFrames for
        Product_SubCategory.
    :return: Sales Master with all the joined DataFrames.
    """
    return (
        sales_master
        .join(calendar_master,
              sales_master["OrderDate"] == calendar_master["Date"],
              how="left")
        .drop(calendar_master["Date"])
        .join(customers_master, on="CustomerKey", how="left")
        .join(products_master, on="ProductKey", how="left")
        .join(
            product_subcategory_master,
            products_master["ProductSubCategoryKey"] == product_subcategory_master["ProductSubCategoryKey"],  # noqa: E501
            how="left"
        )
        .drop (product_subcategory_master["ProductSubCategoryKey"])
        .join(
            product_category_master,
            product_category_master["ProductCategoryKey"] == product_subcategory_master["ProductCategoryKey"],  # noqa: E501
            how="left"
        )
        .drop(product_category_master["ProductCategoryKey"])
    )


def get_wide_sales(
    sales_master: DataFrame,
    calendar_master: DataFrame,
    customers_master: DataFrame,
    products_master: DataFrame,
    product_category_master: DataFrame,
    product_subcategory_master: DataFrame

) -> DataFrame:
    """
    1. Join the Master tables.
    2. Convert the MaritalStatus and Gender to human readable format.
    3. Calculate SalesAmount, HighValueOrder, Profit.


    :param sales_master: Master DataFrames for Sales.
    :param calendar_master: Master DataFrames for Calendar.
    :param customers_master:
        Master DataFrames for Customers.
    :param products_master:
        Master DataFrames for Products.
    :param product_category_master:
        Master DataFrames for Products_Category.
    :param product_subcategory_master:
        Master DataFrames for Product_SubCategory.
    :return: Sales Master with all the joined DataFrames.
    """
    wide_sales_df = _join_master_tables(
        sales_master,
        calendar_master,
        customers_master,
        products_master,
        product_category_master,
        product_subcategory_master
    )

    calculate_sales_amount = sf.col("OrderQuantity") * sf.col("ListPrice")
    calculate_high_value_order = sf.col("SalesAmount") > 10000
    calculate_profit = sf.col(
        "SalesAmount") - (sf.col("StandardCost") * sf.col("OrderQuantity")
                          )
    return (
        wide_sales_df
        .withColumn(
            "MaritalStatus",
            sf.when(sf.col("MaritalStatus") == 1, "Married")
            .when(sf.col("MaritalStatus") == 0, "Single")
            .otherwise(None)
        )
        .withColumn(
            "Gender",
            sf.when(sf.col("Gender") == 1, "Male")
            .when(sf.col("Gender") == 0, "Female")
            .otherwise(None)
        )
        .withColumn(
            "SalesAmount", calculate_sales_amount
        )
        .withColumn(
            "HighValueOrder", calculate_high_value_order
        )
        .withColumn(
            "Profit", calculate_profit
        )
    )
