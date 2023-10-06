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
Rules_failed	no_of_entries
1	174
2	97
3	44
![image](https://github.com/DS-v/Quality-Movie-Data-Ingestion/assets/59478620/3d219484-8b28-4db6-8af1-392b89b6939d)
=> Frequency of a data quality rules failing in total is as follows:
data_qualityfail	_col1
IsComplete "gross"	169
IsComplete "Meta_score"	157
IsComplete "certificate"	101
ColumnValues "certificate" in ["U","A","UA","R","PG-13","PG"] with threshold >= 0.91	67
Uniqueness "no_of_votes" > 0.95	2
Uniqueness "series_title" > 0.95	2
ColumnValues "released_year" in ["1901","1902","1903","1904","1905","1906","1907","1908","1909","1910","1911","1912","1913","1914","1915","1916","1917","1918","1919","1920","1921","1922","1923","1924","1925","1926","1927","1928","1929","1930","1931","1932","1933","1934","1935","1936","1937","1938","1939","1940","1941","1942","1943","1944","1945","1946","1947","1948","1949","1950","1951","1952","1953","1954","1955","1956","1957","1958","1959","1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023"]	1
ColumnLength "released_year" = 4	1
![image](https://github.com/DS-v/Quality-Movie-Data-Ingestion/assets/59478620/e29055d2-d0f0-46dd-9678-6f17fe7fda71)


Note that 	
IsComplete "gross", IsComplete "Meta_score", IsComplete "certificate"
are the top 3 rules failing most number of times. In consulation with upstream team their missing values of such columns can be handled based on a ruleset and perhaps such data can then be re-ingested.

Value of "certificate" column is typically in ["U","A","UA","R","PG-13","PG"], however values like "Accepted" or "Passed" are occuring which needs to be corrected.


## Next Steps:

The pipeline can be automated using triggers, cloudwatch events and lambda functions, so that as soon as new data arrives in s3 bucket, it's crawled and glue job starts it's execution for the increemental load.
A dashboard can be built on athena table for bad data, which will be updated in real time to report results.

