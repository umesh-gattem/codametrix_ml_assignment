import hashlib
import string
from urllib.request import urlopen

import pytest
from pyspark.sql import SparkSession, DataFrame, functions as F


@pytest.fixture(scope="session")
def df() -> DataFrame:
    """
    Pytest Fixture acts like a setup module function where the function gets invoked at the start
    of the execution. The function is expected to return a Spark Dataframe
    """
    # Step 1: Create a Spark Session

    # Step 2: Read the json file, fix data integrity issues, and verify the schema of the data
    """
    Expected Schema

    root
        |-- jira_ticket_id: integer (nullable = true)
        |-- date: date (nullable = true)
        |-- completed: boolean (nullable = true)
        |-- num_slack_messages: integer (nullable = true)
        |-- num_hours: float (nullable = true)
        |-- engineer: string (nullable = true)
        |-- ticket_description: string (nullable = true)
        |-- KPIs: array (nullable = true)
        |    |-- element: struct (containsNull = true)
        |    |    |-- initiative: string (nullable = true)
        |    |    |-- new_revenue: float (nullable = true)
        |-- lines_per_repo: array (nullable = true)
        |    |-- element: map (containsNull = true)
        |    |    |-- key: string
        |    |    |-- value: integer (valueContainsNull = true)
    """

    url = "https://raw.githubusercontent.com/kingspp/mlen_assignment/main/data.json"

    jsonData = urlopen(url).read().decode('utf-8')

    spark = SparkSession.builder.getOrCreate()

    rdd = spark.sparkContext.parallelize([jsonData])
    df = spark.read.json(rdd)
    return df


def hash_util(obj) -> str:
    """Function to return the hash of the object

    Args:
        obj (_type_): Object can be of type string, int, float, Pyspark Row, list of Pyspark Rows,
        any objetct that can be projected in string format

    Returns:
        str: Hash of the object
    """
    return hashlib.md5(str(obj).encode("utf-8")).hexdigest()


def test_count_of_unique_tickets(df):
    """
    Sample Question: Get the count of unique tickets in the data
    ex: 100
    """
    res = df.select("jira_ticket_id").distinct()
    assert hash_util(res.count()) == "14ee22eaba297944c96afdbe5b16c65b"


def test_q1_longest_description(df):
    """
    Question: What is the longest Jira ticket description?

    Return the longest ticket description
    ex: [Row(ticket_description='...')]
    """
    longest_description_row = df.withColumn("description_length", F.length(F.col("ticket_description"))) \
        .orderBy(F.col("description_length").desc())
    longest_description_row = longest_description_row.select("ticket_description").limit(1)
    assert hash_util(longest_description_row.collect()) == "a0ef0d383ff7ee8a3af1669f9a8e0f14"


def test_q2_repo_with_max_lines(df):
    """
    Question: Which repo had the most lines of code added?

    Return the repo with maximum lines of code added
    ex: [Row(repo='...')]
    """
    repos_df = df.selectExpr("inline(lines_per_repo)")

    sum_expr = [F.expr(f"sum({col}) as {col}") for col in repos_df.columns]
    repo_lines_df = repos_df.agg(*sum_expr)
    repo_lines_dict = repo_lines_df.collect()[0].asDict()

    most_lines_repo = max(repo_lines_dict, key=repo_lines_dict.get)
    total_lines = repo_lines_dict[most_lines_repo]
    spark = SparkSession.builder.getOrCreate()
    result_df = spark.createDataFrame([(most_lines_repo, total_lines)], ["repo", "total_lines"]).select("repo")
    assert hash_util(result_df.collect()) == "7f2650ec9b6159c18eba65f65615740d"


def test_q3_max_number_of_slack_messages_per_engineer(df):
    """
    Question: Provide the maximum number of Slack messages in any ticket for each engineer

    Return the maximum number of slack messages per engineer
    ex: [Row(engineer='...', max_messages=...),
        Row(engineer='...', max_messages=...),
                    ...,
        Row(engineer='...', max_messages=...)]

    hint: Results are ordered by engineer
    """
    engineer_messages = df.withColumn("engineer", F.initcap(F.col("engineer"))).filter(F.col("engineer").isNotNull())
    engineer_messages = engineer_messages.groupBy("engineer").agg(F.max("num_slack_messages").alias('max_messages'))
    engineer_messages = engineer_messages.orderBy(F.col("engineer"))
    engineer_messages.collect()
    assert hash_util(engineer_messages.collect()) == "a237ab13f39d30d21b8b937359e9c01f"


def test_q4_mean_hours_spent(df):
    """
    Question: Mean hours spent on a ticket in June 2023

    Return Mean hours spent on a ticket of specific month and year
    ex: [Row(mean_hours=...)]
    """
    df_with_dates = df.withColumn("date_clean", F.to_date(df.date, "yyyy-MM-dd"))
    q4_filtered_df = df_with_dates.where((F.col("date_clean") > '2023-05-31') & (F.col("date_clean") < "2023-07-01"))
    q4_filtered_df = q4_filtered_df.agg(F.mean("num_hours").alias("mean_hours"))
    q4_filtered_df.collect()
    assert hash_util(q4_filtered_df.collect()) == "7facdf09b955d4732ed4138d3fa48778"


def test_q5_total_lines_contributed(df):
    """
    Question: Total lines of code contributed by completed tickets to the repo 'A'

    Return the total_lines_of_code_contributed
    ex: [Row(total=...)]
    """
    line_per_repo = df.selectExpr("inline(lines_per_repo)")
    completed_tickets = line_per_repo.where((F.col("completed") == 'true')).agg(F.sum("A").alias("total"))
    assert hash_util(completed_tickets.collect()) == "6adec64b2a723c9a52024c53068f264d"


def test_q6_total_new_revenue_per_engineer_per_company_initiative(df):
    """
    Question: Total new revenue per engineer per company initiative

    Return the total_lines_of_code_contributed
    ex: [Row(engineer='...', KPIs=[Row(initiative='...', total_revenue=...), Row(initiative='...', total_revenue=...), ...),
                        ...
    Row(engineer='...', KPIs=[Row(initiative='...', total_revenue=...), Row(initiative='...', total_revenue=...), ...)]

    hint: Order the results by engineer. Pay attention to the order of the intitiatives and total_revenue in KPI's
    """
    new_df = df.withColumn("engineer", F.initcap(F.col("engineer"))).filter(F.col("engineer").isNotNull())
    inline_kpis = new_df.select("engineer", F.inline(df['KPIs'].alias('initiative', 'new_revenue')))
    initiative_group_by = inline_kpis.groupBy('engineer', 'initiative')
    revenue_data = initiative_group_by.agg(F.sum('new_revenue').alias('total_revenue'))
    sorted_revenue_data = revenue_data.sort('initiative', ascending=True)
    structed_data = sorted_revenue_data.select('engineer', F.struct('initiative', 'total_revenue').alias('KPIs'))
    grouped_data = structed_data.groupBy('engineer')
    output_data = grouped_data.agg(F.collect_list('KPIs').alias('KPIs'))
    filtered_data = output_data.orderBy(F.col("engineer"))
    assert hash_util(filtered_data.collect()) == "c7b9f4457e8682313464d604f6f66581"
