# Quality-Movie-Data-Ingestion

## Problem Statement:

Movies related data needs to be ingested in a efficient and widely used Data Warehousing solution like Amazon Redshift. However the dataset available is low quality with missing data and having entries irrelevant to business's use case. A system needs to be designed which runs quality checks on data and ingests it into a data warehouse. Additionally bad data needs to be stored and analysed, so that results about faulty data can be communicated to the upstream team.

## Design

![quality_movie_data drawio (1)](https://github.com/DS-v/Quality-Movie-Data-Ingestion/assets/59478620/4cf55e27-efd0-4813-b9f7-8b84c1b6f987)


## Figuring out Data Quality Rules

Ruke recommendations of AWS Glue Data Quality service serve as a good starting point for data quality checks. However one needs to build on those rules and alter them based on input from business team. Most of the rules regarding the complete dataset on completeness, uniqueness, standard deviation of column values were kept as is. Rules regarding values of individual columns were tinkered with. These involve discarding entries with empty entries and making sure values lie in a logical range/set.
Business needs only movies with IMDB rating >= 7 to be stored in warehouse. That change is also added to rules set. Read the text file for completre set of rules.
