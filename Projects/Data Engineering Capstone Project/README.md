# Data Engineering Capstone Project

## Summary

* [Approach](#Approach)
* [Purpose](#Purpose)
* [Datasets](#Datasets)
* [Schema definition](#Schema-definition)
* [How to run](#How-to-run)
* [Data dictionary](#Data-dictionary)
--------------------------------------------

### Approach

To complete this project the following steps were followed:

* Scope the Project and Gather Data
* Explore and Assess the Data
* Define the Data Model
* Run ETL to Model the Data
* Complete Project Write Up


### Purpose

In the project we will create a ETL pipeline to  Data Lake using US I94 Immigration data. We will extract, process, clean and store data that will later on be used to analyse tourist and immigration flow to US through different airports. We could set some expectations on the inflow of the tourist, as well the necessary exchange currencies for tourists. We could extract seasonalities and prepare accordingly.

We had used Python and Apache Spark for their speed and usabillity on this smaller dataset. These tools contained all necesary libraries we used to extract, read, clean, process and store tables. Due to limited dataset available, we had locally stored the fact table using the Spark SQL in parquet files partitioned by city and state. The data could be updated at monthly basis as that is level of aggregation we used here, and the level of aggregation the imigration data is provided.

If the data would increase by 100 times, we would store the input data in the cloud storage AWS S3. To process the data clustered Spark would be used as it would allow parallel processing of the data. We would consider using AWS Redshift to store the staging and the final tables, while the output data would be stored bask to AWS S3.

If the data populates a dashboard that must be updated on a daily basis by 7am every day, we would use Airflow to schedule and run the data pipeline. 

If the database needed to be accessed by 100+ people, we could replicate the data to different nodes used by different users. Moreover, we could store the data in the AWS and use the web app to access the data. If the usage would increase further more we could build a dashboard using some BI tool, e.g. Tableu.

### Datasets

The following datasets are included in the project workspace. We purposely did not include a lot of detail about the data and instead point you to the sources. This is to help you get experience doing a self-guided project and researching the data yourself. If something about the data is unclear, make an assumption, document it, and move on. Feel free to enrich your project by gathering and including additional data sources.

- I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
- World Temperature Data: This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/selfishgene/historical-hourly-weather-data).
- U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
- Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).
- Countries Table: This is a simple table of country codes and additional information about countries. It comes from [here](https://datahub.io/JohnSnowLabs/iso-3166-country-codes-itu-dialing-codes-iso-4217-currency-codes/r/iso-3166-country-codes-itu-dialing-codes-iso-4217-currency-codes-csv.csv).


### Schema definition

There are five dimension tables:
* airport_data - provides information on airports
* country_data - provides information on countries
* demographics_data - provides information on demographics in U.S. cities
* temperature_data - rovides information on average temperatures in U.S. cities
* immigration_data - provides information on arrivals in U.S.

* fact_table - groups the information in the above tables for easier analysis

### How to run

```
mralasic_capstone_project.ipynb # run the Jupyter notebook 
```

### Data dictionary

##### airport_data - data was extracted from airport data downloaded from Datahub
  * airport_id(string) - Unique airport ID code
  * city_name(string) - City airport is located in
  * state_code(string) - Abbriviation of state airport is located in
  * airport_name(string) - Airport name
  * airport_type(string) - Airport type

##### country_data - data was extracted from country data downloaded from Datahub
  * country_name(string) - Country name
  * currency_name(string) - Country currency
  * capital(string) - Country capital
  * continent(string) - Continent country is located in

##### demographics_data - data was extracted from demographics data downloaded from OpenSoft
  * city_name(string) - City name
  * state_code(string) - Abbriviation of state
  * state_name(string) - State name
  * total_population(integer) - Total population of the city
  * male_population(integer) - Male population of the city
  * female_population(integer) - Female population of the city
  * immigrants_population(integer) - Immigrants population of the city
  * median_age(float) - Median age of the population

##### temperature_data - data was extracted from temperature data downloaded from Kaggle
  * city(string) - City name
  * month(integer) - Month
  * average_temperature(double) - Average temperature in degrees Celsius
  * average_temperature_uncertainty(double) - Average temperature uncertanty in degrees Celsius


##### immigration_data - data was extracted from original I94 Immigrations data
  * year(integer) - Year
  * month(integer) - Month
  * origin_country_code(integer) - Country of origin code
  * origin_country(string) - Country of origin name calculated from origin_country_code using SAS Labels Descriptions
  * destination_city_code(string) - Destination city code
  * destination_city_name(string) - Destination city name calculated from destination_city_code using SAS Labels Descriptions
  * destination_state(string) - Abbriviation of destination state
  * age(integer) - Age of traveler listed on I94
  * gender(string) - Gender of traveler listed on I94 
  * visa_type(string) - Visa type
  * visa_purpose(string) - Visa purpose calculated from origin code using SAS Labels Descriptions
  * count(long) Number of arrivals


##### fact_table 
  * Year(integer) - Year
  * month(integer) - Month
  * destination_city_name(string) - Destination city name
  * destination_state(string) - Destination state name
  * number_of_airports(long) - Number of airports in the city
  * total_population(integer) - Total population of the city
  * immigrants_population(integer) - Immigrants population of the city
  * origin_country(string) - Country of origin name
  * origin_continent(string) - Continent of origin
  * origin_currency(string) - Currency in country of origin
  * average_temperature(double) - Average temperature in degrees Celsius
  * visa_purpose(string) - Visa purpose (1-Business 2-Pleasure 3-Student)
  * count(long) - Number of arrivals


