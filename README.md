# Quality-Movie-Data-Ingestion

## Problem Statement:

Movies related data needs to be ingested in a efficient and widely used Data Warehousing solution like Amazon Redshift. However the dataset available is low quality with missing data and having entries irrelevant to business's use case. A system needs to be designed which runs quality checks on data and ingests it into a data warehouse. Additionally bad data needs to be stored and analysed, so that results about faulty data can be communicated to the upstream team.


## Design:

![quality_movie_data drawio (1)](https://github.com/DS-v/Quality-Movie-Data-Ingestion/assets/59478620/4cf55e27-efd0-4813-b9f7-8b84c1b6f987)


## Figuring out Data Quality Rules:

Ruke recommendations of AWS Glue Data Quality service serve as a good starting point for data quality checks. However one needs to build on those rules and alter them based on input from business team. Most of the rules regarding the complete dataset on completeness, uniqueness, standard deviation of column values were kept as is. Rules regarding values of individual columns were tinkered with. These involve discarding entries with empty entries and making sure values lie in a logical range/set.
Business needs only movies with IMDB rating >= 7 to be stored in warehouse. That change is also added to rules set. Read the text file for completre set of rules.


## Bad data and analysis:

A total of 685 out of 1000 records passed the data quality checks.
The failed records have been attached above along with sql queries used for analysis.
=> None of the records failed business criteria of IMDB rating > 7.
=> No of rules failing with respect to no of entries is as follows:

![image](https://github.com/DS-v/Quality-Movie-Data-Ingestion/assets/59478620/dab354e4-0d40-466e-a53b-88356e8de2b9)

=> Frequency of a data quality rules failing in total is as follows:

![image](https://github.com/DS-v/Quality-Movie-Data-Ingestion/assets/59478620/8575ad6e-ae09-4667-a9db-ae2471ee8d30)



Note that 	
IsComplete "gross", IsComplete "Meta_score", IsComplete "certificate"
are the top 3 rules failing most number of times. In consulation with upstream team their missing values of such columns can be handled based on a ruleset and perhaps such data can then be re-ingested.

Value of "certificate" column is typically in ["U","A","UA","R","PG-13","PG"], however values like "Accepted" or "Passed" are occuring which needs to be corrected.


## Next Steps:

The pipeline can be automated using triggers, cloudwatch events and lambda functions, so that as soon as new data arrives in s3 bucket, it's crawled and glue job starts it's execution for the increemental load.
A dashboard can be built on athena table for bad data, which will be updated in real time to report results.

