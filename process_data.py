import sys
import pandas as pd
import numpy as np
import sqlite3
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    """
      Function:
      load data from two csv file and then merge them
      Args:
      messages_filepath (str): the file path of messages csv file
      categories_filepath (str): the file path of categories csv file
      Return:
      df (DataFrame): A dataframe of messages and categories
      """
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = messages.merge(categories, how='inner', on='id')
    return df


def clean_data(df):
    """
      Function:
      clean the Dataframe df
      Args:
      df (DataFrame): A dataframe of messages and categories need to be cleaned
      Return:
      df (DataFrame): A cleaned dataframe of messages and categories
      """

    # Split `categories` into separate category columns.
    categories = df['categories'].str.split(';', expand=True)

    # Cut the last character of each category
    # select the first row of the categories dataframe
    row = categories.head(1)
    category_colnames = row.applymap(lambda x: x[:-2]).iloc[0, :]
    category_colnames = category_colnames.tolist()

    # Rename the columns of `categories`
    categories.columns = category_colnames

    # Convert category values to just numbers 0 or 1.
    for column in categories:
        categories[column] = categories[column].astype(str).str[-1]
        categories[column] = categories[column].astype(int)

    # drop the original categories column from `df`
    df = df.drop(['categories'], axis=1)

    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1, join='inner')

    # Drop the duplicates.
    df.drop_duplicates(inplace=True)

    return df


def save_data(df, database_filename):
    """
       Function:
       Save the Dataframe df in a database
       Args:
       df (DataFrame): A dataframe of messages and categories
       database_filename (str): The file name of the database
       """
    engine = create_engine('sqlite:///{}'.format(database_filename))
    df.to_sql('disaster_messages_tbl', engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories ' \
              'datasets as the first and second argument respectively, as ' \
              'well as the filepath of the database to save the cleaned data ' \
              'to as the third argument. \n\nExample: python process_data.py ' \
              'disaster_messages.csv disaster_categories.csv ' \
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
# import sys
# import pandas as pd
# from sqlalchemy import create_engine
#
#
# def load_data(messages_filepath, categories_filepath):
#     """Load dataframe from filepaths
#     INPUT
#     messages_filepath -- str, link to file
#     categories_filepath -- str, link to file
#     OUTPUT
#     df - pandas DataFrame
#     """
#     messages = pd.read_csv(messages_filepath)
#     categories = pd.read_csv(categories_filepath)
#     df = messages.merge(categories, on='id')
#     return df
#
#
# def clean_data(df):
#     """Clean data included in the DataFrame and transform categories part
#     INPUT
#     df -- type pandas DataFrame
#     OUTPUT
#     df -- cleaned pandas DataFrame
#     """
#     categories = df['categories'].str.split(pat=';', expand=True)
#     row = categories.loc[0]
#     colnames = []
#     for entry in row:
#         colnames.append(entry[:-2])
#     category_colnames = colnames
#     print('Column names:', category_colnames)
#     categories.columns = category_colnames
#     for column in categories:
#         categories[column] = categories[column].str[-1:]
#         categories[column] = categories[column].astype(int)
#     df.drop('categories', axis=1, inplace=True)
#     df = pd.concat([df, categories], axis=1)
#     df.drop_duplicates(inplace=True)
#     # Removing entry that is non-binary
#     df = df[df['related'] != 2]
#     print('Duplicates remaining:', df.duplicated().sum())
#     return df
#
#
# def save_data(df, database_filename):
#     """Saves DataFrame (df) to database path"""
#     name = 'sqlite:///' + database_filename
#     engine = create_engine(name)
#     df.to_sql('Disasters', engine, index=False)
#
#
# def main():
#     """Runs main functions: Loads the data, cleans it and saves it in a database"""
#     if len(sys.argv) == 4:
#
#         messages_filepath, categories_filepath, database_filepath = sys.argv[1:]
#
#         print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
#               .format('C:/Users/59276/Desktop/P1-20220714T150927Z-001/P1/messages.csv', 'C:/Users/59276/Desktop/P1-20220714T150927Z-001/P1/categories.csv'))
#         df = load_data('C:/Users/59276/Desktop/P1-20220714T150927Z-001/P1/messages.csv', 'C:/Users/59276/Desktop/P1-20220714T150927Z-001/P1/categories.csv')
#
#         print('Cleaning data...')
#         df = clean_data(df)
#
#         print('Saving data...\n    DATABASE: {}'.format('C:/Users/59276/Desktop/P1-20220714T150927Z-001/P1/DisasterResponse.db'))
#         save_data(df, 'C:/Users/59276/Desktop/P1-20220714T150927Z-001/P1/DisasterResponse.db')
#
#         print('Cleaned data saved to database!')
#
#     else:
#         print('Please provide the filepaths of the messages and categories ' \
#               'datasets as the first and second argument respectively, as ' \
#               'well as the filepath of the database to save the cleaned data ' \
#               'to as the third argument. \n\nExample: python process_data.py ' \
#               'disaster_messages.csv disaster_categories.csv ' \
#               'DisasterResponse.db')
#
#
# if __name__ == '__main__':
#     main()
# # import sys
# #
# #
# # def load_data(disaster_messages, disaster_categories):
# #     pass
# #
# #
# # def clean_data(df):
# #     pass
# #
# #
# # def save_data(df, DisasterResponse):
# #     pass
# #
# #
# # def main():
# #     if len(sys.argv) == 4:
# #
# #         messages_filepath, categories_filepath, database_filepath = sys.argv[1:]
# #
# #         print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
# #               .format(messages_filepath, categories_filepath))
# #         df = load_data(messages_filepath, categories_filepath)
# #
# #         print('Cleaning data...')
# #         df = clean_data(df)
# #
# #         print('Saving data...\n    DATABASE: {}'.format(database_filepath))
# #         save_data(df, database_filepath)
# #
# #         print('Cleaned data saved to database!')
# #
# #     else:
# #         print('Please provide the filepaths of the messages and categories '\
# #               'datasets as the first and second argument respectively, as '\
# #               'well as the filepath of the database to save the cleaned data '\
# #               'to as the third argument. \n\nExample: python process_data.py '\
# #               'disaster_messages.csv disaster_categories.csv '\
# #               'DisasterResponse.db')
# #
# #
# # if __name__ == '__main__':
# #     main()