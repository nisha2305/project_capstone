import pandas as pd
import json

def create_mappings(f_content, idx):
        """From udacity knowledge base.
        Sources lookup values from the SAS file. """       
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        creating_mapping_file(dic, 'mapping/'+idx+'.txt')
        return dic

def creating_df_from_data_dictionary(f_content, lookup_column, col_name):
    """
    Create a dataframe of different mappings avaialable inside the KB.
    Inputs:-
            f_content :- File object for reading data dictionary.
            lookup_column:- To search for column name inside data dictionary.
            col_name :- Column name to be given to the derived data.
    Output:-
            df :- A dataframe object.
    """
    result = create_mappings(f_content, lookup_column)
    df = pd.DataFrame.from_dict(result, orient='index', columns=[col_name])
    df = df.reset_index()
    df.columns = ['{}_cd'.format(col_name), col_name]
    return df

def creating_mapping_file(input_data, output_filepath):
    with open(output_filepath, 'w') as f:
        f.write(json.dumps(input_data))

def process_port_mapping_file(filename = 'mapping/i94prtl.txt'):        
    """
    The i94prtl file contains port_name along with state separated by commma.
    We can split the value and overwrites the file in same location
    """
    with open(filename, 'r+') as f:
        data = f.read()
        f.seek(0)
        f = json.loads(data)
        port_data = {k:v.split(',')[0] for k,v in f.values()}
        f.write(json.dumps(port_data))
        f.truncate()
        
if __name__ == "__main__":
    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
    df_us_country = create_mappings(f_content, "i94cntyl")  
    df_airlines = create_mappings(f_content, "i94prtl")
    df_us_state = create_mappings(f_content, "i94addrl")
    df_arrival_mode = create_mappings(f_content, "i94model")
    df_visa = create_mappings(f_content, "I94VISA")
    process_port_mapping_file()
