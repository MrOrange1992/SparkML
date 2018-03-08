## SparkML

## Contributors
Lukas Schneider

Felix Rauchenwald

## Setup Instructions
Prerequired Software:
1) IntelliJ IDEA / Eclipse IDE
2) Java 8
3) Spark 2.2.0 or newer
4) Scala 2.10.6
5) (Scala SBT)

For a working Apache Spark 2.2.0 environment the combination of Java 8 and Scala 2.10.6 is needed. This may be object to change in future versions.
To create plots with the scala Plotly library an account for https://plot.ly is needed. If you are using the same configuration as setup in this project, make sure to use the same depency provided in the build.sbt in this repository. 
"co.theasi" %% "plotly" % "0.1"

To use the NBA data sets used in this project an API key for MySportsFeeds (https://www.mysportsfeeds.com) is needed. 

## Introduction
The aim of this project is to analyse the Apache Spark Framework and its capabilities and techniques for a selection of use cases related to Machine Learning. The algorithms and models used can be narrowed down to:
* Linear Regression
*	Logistic Regression
*	Decision Tree
*	Clustering (KMeans)

The data sets used for the application of these models vary in size and detail to test their effectiveness and applicability.  

## Data
### NBA Statistic Data
This data set is used for predicting NBA players points per game based on features like: field goal percentage, position, minutes per game and others. The prediction values are achieved by the implementation of linear regression and decision tree algorithms built into the spark ML and MLlib libraries. The data is obtained from the MySportsFeeds API over HTTP requests. 

### Medien Transparenz
This data set is used for statistical applications and a clustering use case. The data contains payments of Austrian organisations for different media parted in quarters per year starting with quarter four of 2013 until quarter two of 2017. 

### Flight and Weather Data
Data of flights in California USA are combined with weather data for this area to try and predict if flights are delayed. The flight data was obtained from the Bureau of Transportation Statistics.
https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
The weather data was obtained from the National Centers for Environmental Information.
https://www.ncdc.noaa.gov/cdo-web/search
