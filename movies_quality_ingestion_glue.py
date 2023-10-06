import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import concurrent.futures
import re


class GroupFilter:
    def __init__(self, name, filters):
        self.name = name
        self.filters = filters


def apply_group_filter(source_DyF, group):
    return Filter.apply(frame=source_DyF, f=group.filters)


def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {
            executor.submit(apply_group_filter, source_DyF, gf): gf
            for gf in group_filters
        }
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print("%r generated an exception: %s" % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="imdb-rating",
    table_name="movie_rating_imdb",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1695880460926_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
     Rules = [
              RowCount between 500 and 2000,
            IsComplete "poster_link",
            Uniqueness "poster_link" > 0.95,
            ColumnLength "poster_link" between 108 and 162,
            IsComplete "series_title",
            Uniqueness "series_title" > 0.95,
            ColumnLength "series_title" between 1 and 69,
            IsComplete "released_year",
            ColumnValues "released_year" in ["1901", "1902", "1903", "1904", "1905", "1906", "1907", "1908", "1909", "1910", "1911", "1912", "1913", "1914", "1915", "1916", "1917", "1918", "1919", "1920", "1921", "1922", "1923", "1924", "1925", "1926", "1927", "1928", "1929", "1930", "1931", "1932", "1933", "1934", "1935", "1936", "1937", "1938", "1939", "1940", "1941", "1942", "1943", "1944", "1945", "1946", "1947", "1948", "1949", "1950", "1951", "1952", "1953", "1954", "1955", "1956", "1957", "1958", "1959", "1960", "1961", "1962", "1963", "1964", "1965", "1966", "1967", "1968", "1969", "1970", "1971", "1972", "1973", "1974", "1975", "1976", "1977", "1978", "1979", "1980", "1981", "1982", "1983", "1984", "1985", "1986", "1987", "1988", "1989", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022","2023"],
            ColumnLength "released_year" = 4,
            IsComplete "certificate",
            ColumnValues "certificate" in ["U","A","UA","R","PG-13","PG"] with threshold >= 0.91,
            ColumnLength "certificate" <= 8,
            IsComplete "runtime",
            ColumnLength "runtime" between 5 and 8,
            IsComplete "genre",
            ColumnLength "genre" between 4 and 30,
            IsComplete "imdb_rating",
            ColumnValues "imdb_rating" between 7 and 10,
            IsComplete "overview",
            ColumnLength "overview" between 39 and 314,
            IsComplete "Meta_score",
            StandardDeviation "meta_score" between 11.75 and 12.99,
            ColumnValues "meta_score" between 27 and 101,
            IsComplete "director",
            ColumnLength "director" between 6 and 33,
            IsComplete "star1",
            ColumnLength "star1" between 3 and 26,
            IsComplete "star2",
            ColumnLength "star2" between 3 and 26,
            IsComplete "star3",
            ColumnLength "star3" between 3 and 28,
            IsComplete "star4",
            ColumnLength "star4" between 3 and 28,
            IsComplete "no_of_votes",
            StandardDeviation "no_of_votes" between 310848.53 and 343569.43,
            Uniqueness "no_of_votes" > 0.95,
            ColumnValues "no_of_votes" between 25087 and 2343111,
            IsComplete "gross",
            ColumnLength "gross" <= 11  
        ]
"""

EvaluateDataQuality_node1695880460926 = EvaluateDataQuality().process_rows(
    frame=S3bucket_node1,
    ruleset=EvaluateDataQuality_node1695880460926_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1695880460926",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node ruleOutcomes
ruleOutcomes_node1695880720380 = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node1695880460926,
    key="ruleOutcomes",
    transformation_ctx="ruleOutcomes_node1695880720380",
)

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1695880775144 = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node1695880460926,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node1695880775144",
)

# Script generated for node Conditional Router
ConditionalRouter_node1695881254287 = threadedRoute(
    glueContext,
    source_DyF=rowLevelOutcomes_node1695880775144,
    group_filters=[
        GroupFilter(
            name="failed_output",
            filters=lambda row: (
                bool(re.match("Failed", row["DataQualityEvaluationResult"]))
                or bool(re.match("failed", row["DataQualityEvaluationResult"]))
                or bool(re.match("FAILED", row["DataQualityEvaluationResult"]))
            ),
        ),
        GroupFilter(
            name="default_group",
            filters=lambda row: (
                not (
                    bool(re.match("Failed", row["DataQualityEvaluationResult"]))
                    or bool(re.match("failed", row["DataQualityEvaluationResult"]))
                    or bool(re.match("FAILED", row["DataQualityEvaluationResult"]))
                )
            ),
        ),
    ],
)

# Script generated for node failed_output
failed_output_node1695881254415 = SelectFromCollection.apply(
    dfc=ConditionalRouter_node1695881254287,
    key="failed_output",
    transformation_ctx="failed_output_node1695881254415",
)

# Script generated for node default_group
default_group_node1695881254414 = SelectFromCollection.apply(
    dfc=ConditionalRouter_node1695881254287,
    key="default_group",
    transformation_ctx="default_group_node1695881254414",
)

# Script generated for node Change Schema
ChangeSchema_node1695881702941 = ApplyMapping.apply(
    frame=default_group_node1695881254414,
    mappings=[
        ("poster_link", "string", "poster_link", "string"),
        ("series_title", "string", "series_title", "string"),
        ("released_year", "string", "released_year", "string"),
        ("certificate", "string", "certificate", "string"),
        ("runtime", "string", "runtime", "string"),
        ("genre", "string", "genre", "string"),
        ("imdb_rating", "double", "imdb_rating", "double"),
        ("overview", "string", "overview", "string"),
        ("meta_score", "int", "meta_score", "int"),
        ("director", "string", "director", "string"),
        ("star1", "string", "star1", "string"),
        ("star2", "string", "star2", "string"),
        ("star3", "string", "star3", "string"),
        ("star4", "string", "star4", "string"),
        ("no_of_votes", "int", "no_of_votes", "int"),
        ("gross", "string", "gross", "string"),
    ],
    transformation_ctx="ChangeSchema_node1695881702941",
)

# Script generated for node Amazon S3
AmazonS3_node1695881164928 = glueContext.write_dynamic_frame.from_options(
    frame=ruleOutcomes_node1695880720380,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://imdb-dataquality/rules-outcome/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1695881164928",
)

# Script generated for node Amazon S3
AmazonS3_node1695881567005 = glueContext.write_dynamic_frame.from_options(
    frame=failed_output_node1695881254415,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://imdb-dataquality/bad-records/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1695881567005",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1695881914925 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1695881702941,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-683934273415-ap-south-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "imdb.imdb_movies_rating",
        "connectionName": "glue2redshift",
        "preactions": "CREATE TABLE IF NOT EXISTS imdb.imdb_movies_rating (poster_link VARCHAR, series_title VARCHAR, released_year VARCHAR, certificate VARCHAR, runtime VARCHAR, genre VARCHAR, imdb_rating DOUBLE PRECISION, overview VARCHAR, meta_score INTEGER, director VARCHAR, star1 VARCHAR, star2 VARCHAR, star3 VARCHAR, star4 VARCHAR, no_of_votes INTEGER, gross VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1695881914925",
)

job.commit()
