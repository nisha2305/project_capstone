## Project Scope

#### Scope the Project and Gather Data

- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. immigration_data_sample.csv contains the sample data.
- **U.S. City Demographic Data**: This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey. 
- **Airport Code Table**: This data comes from [ourairports](http://ourairports.com/data/airports.csv). It is a simple table of airport codes and corresponding cities. It contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.

#### Explore and Assess the Data
- Using the pandas profiling python library, explore the data for unique, missing, distinct count and duplicate values. 
  we can see this distribution for data at each column level and get an understanding of what data point is most frequent. 
  Thus its giving us a quick glance at the data.
  **PATH** 
  profiling/
  **Files**
  - airport_pandas_profiling.html
  - demographic_pandas_profiling.html
  - immigration_pandas_profiling.html
  
##### Immigration Data
- I94YR - 4 digit year
- I94MON - Numeric month
- I94CIT & I94RES - This format shows all the valid and invalid codes for processing
- I94PORT - This format shows all the valid and invalid codes for processing
- ARRDATE is the Arrival Date in the USA. It is a SAS date numeric field that a 
   permament format has not been applied.  Please apply whichever date format 
   works for you.
- I94MODE - There are missing values as well as not reported (9)
- I94ADDR - There is lots of invalid codes in this variable and the list below 
   shows what we have found to be valid, everything else goes into 'other'
- DEPDATE is the Departure Date from the USA. It is a SAS date numeric field that 
   a permament format has not been applied.  Please apply whichever date format 
   works for you.
- I94BIR - Age of Respondent in Years
- I94VISA - Visa codes collapsed into three categories
- COUNT - Used for summary statistics
- DTADFILE - Character Date Field - Date added to I-94 Files - CIC does not use
- VISAPOST - Department of State where where Visa was issued - CIC does not use
- OCCUP - Occupation that will be performed in U.S. - CIC does not use
- ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use
- ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use
- ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use
- MATFLAG - Match flag - Match of arrival and departure records
- BIRYEAR - 4 digit year of birth
- DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use
- GENDER - Non-immigrant sex
- INSNUM - INS number
- AIRLINE - Airline used to arrive in U.S.
- ADMNUM - Admission Number
- FLTNO - Flight number of Airline used to arrive in U.S.
- VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

#### Steps
- Process the **countries** data from i94cntyl.txt (the file is created from the data dictionary        'I94_SAS_Labels_Descriptions.SAS' using mapper.py file) and then saved to parquet files 
- Process the **us_states** data from i94addrl.txt (the file is created from the data dictionary        'I94_SAS_Labels_Descriptions.SAS' using mapper.py file) and then saved to parquet files 
- Process the **us_ports** data from i94prtl.txt (the file is created from the data dictionary        'I94_SAS_Labels_Descriptions.SAS' using mapper.py file) -  splitting city and state and then save them to parquet files 
- Process the mappings tables **visa type** (I94VISA.txt) and **arrival mode** (i94model.txt) that will be later used for joining with the **immigration_data**
- Process and clean **us_airports**, filtering only US airports as we are only considering the us immigration data, data splitting country and state from the iso_region column. Saving the data to parquet files, partitioning them by State
- Process and clean **us-cities-demographics**, and then save them to parquet files, partitioning them by state
- Process and clean **immigration_data**, get datetime from arrdate column value, all missing states or states that cannot be mapped to us_states are set to 99, get the arrival mode and the visa type from the mappings and then save them to 2 sets of parquet files, partitioning them by 1) year, month and us_state and 2) by year, month, arrival_mode,  and port

#### Purpose

The resulting tables:
![schema](./Immigration.PNG)
The purpose of the model is to analyze the immigration data and the connections between the arrival ports and ports and where the immigrants settle (which can be analyzed in connection to the us_cities_demographics).

## Addressing Other Scenarios
The write up describes a logical approach to this project under the following scenarios:

#### The data was increased by 100x.
We can use dask for that. Dask supports the pandas dataframe and Numpy array structures to analyze large datasets. 
Basically, Dask lets you scale pandas and numpy with minimum changes in your code format.

#### The data populates a dashboard that must be updated on a daily basis by 7am every day.
In this case, the ETL process should be scheduled with Airflow (or a similar tool) and the immigration data set should be updated daily. Since the other datasets wouldn't get updated as often, not all the data transformations will run daily.

#### The database needed to be accessed by 100+ people.
We can use a cluster manager such as Yarn and increase the number of nodes depending on the needs.

## Defending Decisions

#### Clearly state the rationale for the choice of tools and technologies for the project. 
For the implementation (mappings) transformations, pandas was used, as it provides easier methods than Spark for regex transformations. 
However for larger datsets where one machine cannot handle all the data in memory, we can use dask for this scenario.
Dask is a flexible library for parallel computing in Python. Dask Composed of two parts. Dynamic task scheduling which performs the various optimized operation and big data collection such as a parallel array, Dataframes, lists.
Dask reuses Pandas API and memory model and thus easier to integrate into current implementation.

Immigration dataset contains many columns like country, state etc which contains coded value and their meaning is depicted in
the data dictionary provided by Udacity in their workspace.
Separate file for mapping of country_code and countrty_name are created using data dictionary and stored inside mapping/ folder and this files will be used for the lookup of value. i94cntyl.txt, i94addrl.txt, i94prtl.txt are processed and stored inside paraquet. 

Demographics data is processed and divided into two files one for specifically demographics race as data in other columns are duplicates. So, it is perfect scenario to divide the datasets to remove data redundancy.

Airport data is processed and columns like iso_region, coordinates are splitted and new columns are created to store the value.
The data is saved to parquet files and sent to S3 so that is can be easily retrieved for further analysis.

#### Propose how often the data should be updated and why.
The immigration data should be updated monthly. 
The other data sets can be updated on a quarterly basis, or more often, if necessary (new countries/ US airports/ US ports appear). 
The demographics data can be updated on a yearly basis - since the data is not pubicly available more often.
However, if data needs to be analyzed more often (such as in the scenario where the dashboard that must be updated on a daily basis by 7am every day), the data should be updated daily.

## Config file (dl.cfg) structure
Below is the structure of the config file. Don't use any quotes for the KEY ID and ACCESS KEY:
```
[AWS]
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
S3=''
```