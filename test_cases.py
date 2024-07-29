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
    max_length = 0
    long_ticket = None

    for row in df.rdd.toLocalIterator():
        if len(row["ticket_description"]) > max_length:
            max_length = len(row["ticket_description"])
            long_ticket = row['ticket_description']

    spark = SparkSession.builder.getOrCreate()

    res = spark.createDataFrame([{'ticket_description': long_ticket}])
    assert hash_util(res.collect()) == "a0ef0d383ff7ee8a3af1669f9a8e0f14"


def test_q2_repo_with_max_lines(df):
    """
    Question: Which repo had the most lines of code added?

    Return the repo with maximum lines of code added
    ex: [Row(repo='...')]
    """
    all_upper_chars = {char: 0 for char in list(string.ascii_uppercase)}

    for row in df.rdd.toLocalIterator():
        for repo in row['lines_per_repo']:
            for char in all_upper_chars:
                if repo[char]:
                    all_upper_chars[char] += repo[char]

    max_repo = max(all_upper_chars, key=all_upper_chars.get)

    spark = SparkSession.builder.getOrCreate()

    res = spark.createDataFrame([{'repo': max_repo}])
    assert hash_util(res.collect()) == "7f2650ec9b6159c18eba65f65615740d"


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
    # Filter Engineer not null
    engineer_messages = df.withColumn("engineer", F.lower(F.col("engineer"))).filter(F.col("engineer").isNotNull())
    # Max slack messages group by engineer
    engineer_messages = engineer_messages.groupBy("engineer").agg(F.max("num_slack_messages").alias('max_messages'))
    # Convert engineer name to title
    engineer_messages = engineer_messages.withColumn("engineer", F.initcap(F.col("engineer")))
    # Order by engineer
    engineer_messages = engineer_messages.orderBy(F.col("engineer"))
    assert hash_util(engineer_messages.collect()) == "a237ab13f39d30d21b8b937359e9c01f"


def test_q4_mean_hours_spent(df):
    """
    Question: Mean hours spent on a ticket in June 2023

    Return Mean hours spent on a ticket of specific month and year
    ex: [Row(mean_hours=...)]
    """
    df_with_dates = df.withColumn("date_clean", F.to_date(df.date, "yyyy-MM-dd"))
    filtered_df = df_with_dates.where((F.col("date_clean") > '2023-05-31') & (F.col("date_clean") < "2023-07-01"))
    a = filtered_df.agg(F.avg("num_hours").alias("mean_hours"))
    res = a
    #
    # result = 397287.8618578641
    # spark = SparkSession.builder.getOrCreate()
    # rows = [{"mean_hours": result/(8070)}]
    # res = spark.createDataFrame(rows)
    print(res.collect())

    # june_tickets_df = df.filter((F.month(F.col("date")) == 6) & (F.year(F.col("date")) == 2023))

    # Calculate mean hours spent on a ticket in June 2023
    # mean_hours_june_2023 = june_tickets_df.agg(F.avg("num_hours").alias("mean_hours")).first()
    # res = mean_hours_june_2023
    # print(res.collect())
    assert hash_util(res.collect()) == "7facdf09b955d4732ed4138d3fa48778"


def test_q5_total_lines_contributed(df):
    """
    Question: Total lines of code contributed by completed tickets to the repo 'A'

    Return the total_lines_of_code_contributed
    ex: [Row(total=...)]
    """
    repo_a = 0

    for row in df.rdd.toLocalIterator():
        if row['completed'] in ["yes", "true"]:
            for repo in row['lines_per_repo']:
                if repo["A"]:
                    repo_a += repo["A"]

    spark = SparkSession.builder.getOrCreate()

    res = spark.createDataFrame([{'total': repo_a}])
    print(res.collect())
    assert hash_util(res.collect()) == "6adec64b2a723c9a52024c53068f264d"


def test_q6_total_new_revenue_per_engineer_per_company_initiative(df):
    """
    Question: Total new revenue per engineer per company initiative

    Return the total_lines_of_code_contributed
    ex: [Row(engineer='...', KPIs=[Row(initiative='...', total_revenue=...), Row(initiative='...', total_revenue=...), ...),
                        ...
    Row(engineer='...', KPIs=[Row(initiative='...', total_revenue=...), Row(initiative='...', total_revenue=...), ...)]

    hint: Order the results by engineer. Pay attention to the order of the intitiatives and total_revenue in KPI's
    """
    from collections import OrderedDict
    engineer_revenue = OrderedDict()

    for row in df.rdd.toLocalIterator():
        if row['engineer']:
            engineer = row['engineer'].lower()
        else:
            continue

        for initiative in row['KPIs']:
            if engineer not in engineer_revenue:
                engineer_revenue[engineer] = {initiative["initiative"]: initiative["new_revenue"]}
            else:
                engineer_revenue[engineer][initiative["initiative"]] = engineer_revenue[engineer].get(
                    initiative["initiative"], 0) + initiative["new_revenue"]

    result_list = []
    for engineer, kpis in sorted(engineer_revenue.items()):
        result_list.append({"engineer": engineer.title(),
                            "KPIs": [{"initiative": initiative, "total_revenue": revenue} for initiative, revenue
                                     in sorted(kpis.items())]})

    spark = SparkSession.builder.getOrCreate()

    res = spark.createDataFrame(result_list)
    print(res.collect())
    assert hash_util(res.collect()) == "c7b9f4457e8682313464d604f6f66581"
