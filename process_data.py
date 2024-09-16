# Data processing script for disaster response data
# Kofi Osei-Bonsu
# 09/07/2024


import pandas as pd
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO,  # Set logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Customize log format

#FIXME path information? glob?
def data_pre_processing():
    messages = pd.read_csv('data/messages.csv')
    categories = pd.read_csv('data/categories.csv')

    logging.info(f"Data download successful")
    # clean categories
    categories = categories.categories.str.split(';', expand = True)
    # get column names from first row
    category_colnames = categories.iloc[0].str.split('-').str[0].tolist()
    categories.columns = category_colnames
    #Extract value for each column
    for column in categories:
    # set each value to be the last character of the string
        categories[column] = categories[column].str.split('-').str[1]
        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column])

    # merge the two dataframes on index

    # concatenate the original dataframe with the new `categories` dataframe
    df = messages.merge(categories, left_index = True, right_index = True)

    # deduplicate
    df.drop_duplicates(keep='first', inplace=True)
    # remove nan
    df.dropna(inplace=True)

    # remove non english text

    df = df[df.related != 2]

    logging.info(f"Data Cleaning Successful")

    return df

    
def save_date_to_db(df):
    engine = create_engine('sqlite:///DisasterResponse.db')
    df.to_sql('Messages', engine, index=False, if_exists = 'replace')
    logging.info(f"Data stored in db")

# def further_processing(df):
#     df.messages = df

def main():
    df = data_pre_processing()
    save_date_to_db(df)

if __name__ == "__main__":
    main()