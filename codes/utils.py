import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame


def create_spark_session(master_url, packages=None):
    """
    Creates a local spark session
    :param master_url: IP address of the cluster you want to submit the job to or local with all cores
    :param packages: Any external packages if needed, only when called. This variable could be a string of the package
        specification or a list of package specifications.
    :return: spark session object
    """
    if packages:
        packages = ",".join(packages) if isinstance(packages, list) else packages
        spark = (
            SparkSession.builder.master(master_url)
                .config("spark.io.compression.codec", "snappy")
                .config("spark.ui.enabled", "false")
                .config("spark.jars.packages", packages)
                .getOrCreate()
        )
    else:
        spark = (
            SparkSession.builder.master(master_url)
                .config("spark.io.compression.codec", "snappy")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
        )

    return spark


def read_csv_spark(spark, file_path):
    df = (
        spark.read.format("com.databricks.spark.csv")
            .options(header="true", inferSchema="true")
            .load(file_path)
    )
    return df


def unionAll(dfs):
    return reduce(DataFrame.union, dfs)


def spark_date_parsing(df, date_column, date_format):
    """
    Parses the date column given the date format in a spark dataframe
    NOTE: This is a Pyspark implementation
    Parameters
    ----------
    :param df: Spark dataframe having a date column
    :param date_column: Name of the date column
    :param date_format: Simple Date Format (Java-style) of the dates in the date column
    Returns
    -------
    :return: A spark dataframe with a parsed date column
    """
    # Check if date has already been parsed, if it has been parsed we don't need to do it again
    date_conv_check = [
        x[0] for x in df.dtypes if x[0] == date_column and x[1] in ["timestamp", "date"]
    ]
    """
    # mm represents minutes, MM represents months. To avoid a parsing mistake, a simple check has been added
    The logic here is, if the timestamp contains minutes, the date_format string will be greater than 10 letters. 
    Minimum being yyyy-MM-dd which is 10 characters. In this case, if mm is entered to represent months, 
    the timestamp gets parsed for minutes, and the month falls back to January
    """
    if len(date_format) < 12:
        date_format = date_format.replace("mm", "MM")

    if not date_conv_check:
        df = df.withColumn(date_column, F.to_timestamp(F.col(date_column), date_format))
    # Spark returns 'null' if the parsing fails, so first check the count of null values
    # If parse_fail_count = 0, return parsed column else raise error
    parse_fail_count = df.select(
        ([F.count(F.when(F.col(date_column).isNull(), date_column))])
    ).collect()[0][0]
    if parse_fail_count == 0:
        return df
    else:
        raise ValueError(
            f"Incorrect date format '{date_format}' for date column '{date_column}'"
        )


def h2o_aggregate_columns(df, groupby_cols, aggregator_dict):
    """
    Rolls up a dataframe, grouped by a list of columns, and the aggregator_dict contains columns with their aggregation
    type (possibly sum, average, minimum and maximum).
    *** TO BE DEPRECATED ***
    :param df: An H2o Dataframe
    :param groupby_cols: List of columns to make groups out of
    :param aggregator_dict: A dict such as {colName: AggType}
        for e.g. {"Sales": "sum", "Distribution": "max", "pct_growth": "mean"}
    :return: An aggregated H2o dataframe with groupby_cols, and the aggregated columns from the aggregator_dict
    """

    # aggType could be any of [min, max, sum, mean]
    supported_aggregations = ["min", "max", "sum", "mean"]
    if not isinstance(aggregator_dict, dict):
        raise TypeError("Pass a valid dict in {'colName': 'aggType'} format")
    # if input agg_type comes in as 'avg', replace it to H2o compliant 'mean'
    for col_name, agg_type in aggregator_dict.items():
        if agg_type == "avg":
            aggregator_dict[col_name] = "mean"

    if not set(supported_aggregations).issuperset(list(aggregator_dict.values())):
        raise ValueError("Unsupported aggregation passed.")

    updated_agg_dict = {
        i: [k for (k, v) in aggregator_dict.items() if v == i]
        for i in supported_aggregations
    }

    # Add more supported aggregations here if they are added to the supported aggregations list
    df_agg = (
        df.group_by(groupby_cols)
            .min(updated_agg_dict["min"], na="rm")
            .max(updated_agg_dict["max"], na="rm")
            .sum(updated_agg_dict["sum"], na="rm")
            .mean(updated_agg_dict["mean"], na="rm")
            .get_frame()
    )

    def correct_column_names(aggregated_column_name):
        og_column_name = (
            aggregated_column_name.replace("sum_", "")
                .replace("mean_", "")
                .replace("max_", "")
                .replace("min_", "")
        )
        return og_column_name

    df_agg = df_agg.set_names(
        groupby_cols
        + [correct_column_names(x) for x in df_agg.columns if x not in groupby_cols]
    )

    return df_agg


def spark_aggregate_columns(df, groupby_cols, aggregator_dict):
    """
    Rolls up a spark dataframe, grouped by a list of columns, and the aggregator_dict contains columns with their
    aggregation type (possibly sum, average, minimum and maximum).
    Parameters
    ==========
    :param df: a spark dataframe
    :param groupby_cols: list of columns to make groups out of
    :param aggregator_dict: a dict such as {colName: AggType}
        for example:{'Sales':'sum','Distribution':'max','pct_growth':'mean'}
    Returns
    =======
    An aggregated spark dataframe with groupby_cols and the aggregated columns from the aggregator_dict
    """

    # aggType could be any of [min, max, sum, avg, last]
    supported_aggregations = ["min", "max", "sum", "avg", "last"]

    if not isinstance(aggregator_dict, dict):
        raise TypeError("Pass a valid dict in {'colName':'aggType'}")
    # if input agg_type comes in as 'mean', replace it to spark compliant 'avg'
    for col_name, agg_type in aggregator_dict.items():
        if agg_type == "mean":
            aggregator_dict[col_name] = "avg"

    if not set(supported_aggregations).issuperset(list(aggregator_dict.values())):
        raise ValueError("Unsupported aggregation passed.")

    # Add more supported aggregations here if they are added to the supported aggregation list
    df_agg = df.groupBy(groupby_cols).agg(aggregator_dict)

    # Correcting column names
    for col_name, agg_type in aggregator_dict.items():
        df_agg = df_agg.withColumnRenamed(
            "{0}({1})".format(agg_type, col_name), col_name
        )
    return df_agg


def pandas_aggregate_columns(df, groupby_cols, aggregator_dict):
    """
    Rolls up a pandas dataframe, grouped by a list of columns, and the aggregator_dict contains columns with their
    aggregation type (possibly sum, average, minimum and maximum).
    Parameters
    ==========
    :param df: a pandas dataframe
    :param groupby_cols: list of columns to make groups out of
    :param aggregator_dict: a dict such as {colName: AggType}
        for example:{'Sales':'sum','Distribution':'max','pct_growth':'mean'}
    Returns
    =======
    An aggregated pandas dataframe with groupby_cols and the aggregated columns from the aggregator_dict
    """
    # aggType could be any of [min, max, sum, avg]
    supported_aggregations = ["min", "max", "sum", "mean"]

    if not isinstance(aggregator_dict, dict):
        raise TypeError("Pass a valid dict in {'colName':'aggType'}")
    # if input agg_type comes in as 'mean', replace it to spark compliant 'avg'
    for col_name, agg_type in aggregator_dict.items():
        if agg_type == "avg":
            aggregator_dict[col_name] = "mean"
    if not set(supported_aggregations).issuperset(list(aggregator_dict.values())):
        raise ValueError("Unsupported aggregation passed.")

    # Add more supported aggregations here if they are added to the supported aggregation list
    df_agg = df.groupby(groupby_cols).agg(aggregator_dict).reset_index()
    return df_agg


def find_date_range(df, date_column):
    """
    Finds the minimum and maximum date of a date column in a DF
    :param df: A spark dataframe
    :param date_column: A parsed date column name
    :return: Min and Max of the date column (as a tuple of datetime.datetime() objects)
    """
    # Find the date-range
    dates = df.select([F.min(date_column), F.max(date_column)]).collect()[0]
    min_date, max_date = (
        dates["min(" + date_column + ")"],
        dates["max(" + date_column + ")"],
    )
    return min_date, max_date
