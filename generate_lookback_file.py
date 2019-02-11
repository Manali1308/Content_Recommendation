#!/home/ubuntu/anaconda/bin/ipython
"""Script will generate lookback flat files.
    Lookback files are files which contains informations about which
    tbids are viewed by a user in past 2 months.
    Lookback file is used in further code to remove the recommendations 
    which were already seen by user in past 2 months.
    2 lookback files are generated: Resource center and non resource center.
"""

###################################################################
import pandas as pd
from config import *
from utils import subset_columns, load_file_and_select_columns
import datetime as dt


# from data_preprocessor import subset_columns
###################################################################


def main():
    '''Wrapper function to generate lookback file.'''

    # import activity file
    df_activity = load_file_and_select_columns(path_activity, sepr)
    # subset data for past 60 days
    df_activity[timestamp] = pd.to_datetime(df_activity[timestamp])
    max_date = df_activity[timestamp].max()
    last_date_to_consider = max_date - dt.timedelta(days=60)
    # subset activity
    df_activity = df_activity[df_activity[timestamp] >= last_date_to_consider]
    # subset rc and non rc activity
    df_rc_activity = df_activity[df_activity[rc_flag] == 1]
    df_non_rc_activity = df_activity[df_activity[rc_flag] == 0]
    # subset columns
    df_rc_activity = subset_columns(
        df_rc_activity, cols_lookback_file).drop_duplicates()
    df_non_rc_activity = subset_columns(
        df_non_rc_activity, cols_lookback_file).drop_duplicates()
    # save the files
    df_rc_activity.to_csv(path_rc_lookback_file, sep=sepr, index=False)
    df_non_rc_activity.to_csv(path_non_rc_lookback_file, sep=sepr, index=False)


if __name__ == '__main__':
    main()
