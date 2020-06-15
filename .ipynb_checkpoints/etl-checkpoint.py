from dask import dataframe as dd
#import configparser
import numpy as np
import pandas as pd
import os
import json
import boto3

#config = configparser.ConfigParser()
#config.read('dwh.cfg')
#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
#S3_BUCKET = config.get('AWS','S3')

def creating_df_from_data_dictionary(file_path, col_name, dtype = None):
    """
    Create a dataframe of different mappings avaialable inside the KB.
    Inputs:-
            f_content :- File object for reading data dictionary.
            lookup_column:- To search for column name inside data dictionary.
            col_name :- Column name to be given to the derived data.
    Output:-
            df :- A dataframe object.
    """
    f_content = _read_mapping_file(file_path)
    df = pd.DataFrame.from_dict(f_content, orient='index', columns=[col_name])
    df = df.reset_index()
    df.columns = ['{}_code'.format(col_name), col_name]
    if dtype:
        df = df.astype(dtype = dtype)
    return df

def _read_mapping_file(file_path):
    '''
    TO read mapping file
    Inputs:-
        file_name :-  file path of mapping sheet
    Outputs:-
        f :- dict object
    '''
    with open(file_path) as f:
        data = f.read()
        f = json.loads(data)
        return f


def preprocess_immigration_data(df_arrival_mode, df_visa,output_path):
    #fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    #immigration = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
    
    immigration = pd.read_csv('input_data/immigration_data_sample.csv')
    #immigration = dd.from_pandas(immigration, npartitions=3)
    print(immigration.dtypes)
    # drop unneccessary columns
    #print(immigration.isna().mean().round(4) * 100)

    immigration.drop(columns=['count'], inplace=True)
    rows_before_transformation = immigration.shape[0]
    print(immigration['biryear'].value_counts())
    # Defining data type of columns
    
    immigration.rename(columns={'i94yr':'year',
                                'i94mon':'month',
                                'i94cit':'birth_country',
                                'i94res':'resident_country',
                                'i94port':'port',
                                'arrdate':'arrival_date',
                                'i94mode':'arrival_mode',
                                'i94addr':'state',
                                'depdate':'departure_date',
                                'i94bir':'respondent_age',
                                'i94visa':'visa',
                                'occup':'occupation',
                                'entdepa':'arrival_flag',
                                'entdepd':'departure_flag',
                                'entdepu':'update_flag',
                                'matflag':'arri_depart_match',
                                'biryear':'birth_year',
                                'dtaddto':'date_till_allowed',
                                'admnum':'admmision_number',
                                'fltno':'fltght_number'
                               }, inplace = True)
    
    # Converting arrival_mode and visa code as per the data mapping created from data dictionary.
    immigration['arrival_mode'] = immigration['arrival_mode'].apply(lambda x : 
                                df_arrival_mode.loc[df_arrival_mode['mode_code']==x]['mode'].values)
    print(immigration['arrival_mode'].value_counts())
    
    immigration['visa'] = immigration['visa'].apply(lambda x : 
                                df_visa.loc[df_visa['visa_code']==x]['visa'].values[0])
    
    # Tranforming arrival_date to timestamp column
    immigration['arrival_date'] = pd.to_timedelta(immigration['arrival_date'], unit='d') + pd.datetime(1960, 1, 1)
    # Tranforming departure_date to timestamp column
    immigration['departure_date'] = pd.to_timedelta(immigration['departure_date'], unit='d') + pd.datetime(1960, 1, 1)
    # Tranforming dtadfile to timestamp column
    immigration['dtadfile'] = pd.to_datetime(immigration['dtadfile'].astype(str), format='%Y-%m-%d')
    
    # To replace D/S value in column which is not date
    immigration['date_till_allowed'] = immigration['date_till_allowed'].replace('D/S',np.nan)
    # Tranforming date_till_allowed to timestamp column
    immigration['date_till_allowed'] = pd.to_datetime(immigration['date_till_allowed'].astype(str), format='%m%d%Y')
    #print(immigration[~immigration['date_till_allowed'].isnull()][['date_till_allowed']])
    rows_after_transformation = immigration.shape[0]
    _data_quality_check(rows_before_transformation, rows_after_transformation)
    immigration.to_parquet(output_path, partition_cols = ['year','month'], index=False, engine='pyarrow')
       
def preprocess_airport_data(output_path):
    """
    To process the airport data and perform transformation to load the data in final output.
    Input:- 
        output_path:- The output path where to store the data
    """
    # Reading data from airport data
    airport = pd.read_csv('input_data/airport-codes_csv.csv')
    
    # To filter airline data for only US state.
    airport = airport.loc[airport["iso_country"] == 'US']
    rows_before_transformation = airport.shape[0]
    airport.drop_duplicates(inplace=True)
    airport['longitude'] = airport['coordinates'].apply(lambda x: x.split(',')[0])
    airport['latitude'] = airport['coordinates'].apply(lambda x: x.split(',')[1])
    airport['state'] = airport['iso_region'].apply(lambda x: x.split('-')[1])
    rows_after_transformation = airport.shape[0]
    airport.drop(columns=['coordinates','iso_region','iso_country'], inplace=True)
    _data_quality_check(rows_before_transformation, rows_after_transformation)
    airport.to_parquet(output_path, partition_cols = ['state'], index=False, engine='pyarrow')
    
    
def preprocess_demographics_data(output_path):
    demographics = pd.read_csv('input_data/us-cities-demographics.csv',delimiter=';')
    demographics_race = demographics[['State','City','Race','Count']]
    demographics.drop(columns = ['Race','Count'], inplace = True)
    demographics.drop_duplicates(inplace=True)
    demographics_race.drop_duplicates(inplace=True)
    rows_before_transformation = demographics.shape[0]
    demographics[['State','City']].drop_duplicates(inplace=True)
    rows_after_transformation = demographics.shape[0]
    _data_quality_check(rows_before_transformation, rows_after_transformation)
    demographics.to_parquet(output_path, partition_cols = ['State','City'], index=False, engine='pyarrow')
    demographics_race.to_parquet(output_path+'_race', partition_cols = ['State','City','Race'], index=False, engine='pyarrow')
    
    
def _data_quality_check(rows_before_transformation, rows_after_transformation):
    # Data quality check for count of values before and after transformation
    if rows_before_transformation == rows_after_transformation:
        print('data quality check passed')
    else:
        print('data quality check failed')

def upload_files(s3, S3_BUCKET, path):
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                S3_BUCKET.put_object(Key=full_path[len(path)+1:], Body=data)

def main():
    KEY = config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    S3_BUCKET = config.get('AWS','S3')
    session = boto3.Session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)

if __name__ == "__main__":
    
    df_port = creating_df_from_data_dictionary('mapping/i94prtl.txt', 'port')
    
    df_us_country = creating_df_from_data_dictionary(
                                    'mapping/i94cntyl.txt', 'country'
                                    ,dtype= {"country_code":"float64", "country":"object"}
                                    )
    
    df_state = creating_df_from_data_dictionary('mapping/i94addrl.txt', 'state')
    
    df_arrival_mode = creating_df_from_data_dictionary(
                                    'mapping/i94model.txt', 'mode'
                                    ,dtype= {"mode_code":"float64", "mode":"object"}
                                    )
    
    df_visa = creating_df_from_data_dictionary(
                                    'mapping/I94VISA.txt', 'visa'
                                    ,dtype= {"visa_code":"float64", "visa":"object"}
                                    )
    
    preprocess_immigration_data(df_arrival_mode, df_visa,'data/immigration')
    preprocess_airport_data('data/airport')
    preprocess_demographics_data('data/demographics')
    