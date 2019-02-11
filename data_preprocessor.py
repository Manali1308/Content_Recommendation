#!/home/ubuntu/anaconda/bin/ipython
"""
Script will preprocess the daily files which are replaced for the recommendation system and will save it in store_inputs folder.
This also contains functions used to preprocess activity and onsite feedback files. These functions are imported by data_importer.
In the preprocessing steps, we load the file from store_dailyfeeds folder as pandas dataframes, preprocess the dataframes and save the dataframe as flat files in store_inputs folder.

The daily files which get preprocessed are:
mpt_ResourceCenter_Metadata, mpt_content_metadata,mpt_demo,mpt_approvedlinks,mpt_ResourceCenter_Targets
"""

##########################################################################
from config import *
import os
import pandas as pd
from utils import *


##########################################################################


def remove_null_content(df_meta, col_content):
    '''Function will filter metadata for not null content.

    Keyword arguments:
    df_meta -- pandas dataframe
    col_content -- column name(string) which contains content.

    Output -- pandas dataframe
    '''

    df_meta = df_meta[~df_meta[col_content].isnull()]
    return df_meta


def preprocess_metadata(df_metadata, cols_metadata, text_column_name, article_id):
    """Function is used for preprocessing non resource center metadata.

    Keyword arguments:
    df_metadata -- pandas dataframe for non rc metadata
    cols_metadata -- columns used for subsetting df_metadata
    text_column_name -- the name of the column which contain the content of the articles
    article_id -- name of the column which represent article id.
    """

    df_metadata = subset_columns(df_metadata, cols_metadata)
    df_metadata = remove_null_content(df_metadata, text_column_name)
    df_metadata = combine_keys(df_metadata, article_id, 0, False)
    return df_metadata.drop_duplicates()


def preprocess_demo(df_demo, cols_demo):
    """Function is used for preprocessing user demographics file.

    Keyword arguments:
    df_demo -- pandas dataframe for demo file.
    cols_demo -- columns used for subsetting df_demo
    """

    return subset_columns(df_demo, cols_demo).drop_duplicates()


def preprocess_rc_targets(df_rc_targets, cols_rc_targets):
    """Function is used for preprocessing rc targets file.

    Keyword arguments:
    df_rc_targets -- pandas dataframe for resource center targets file.
    cols_rc_targets -- columns used for subsetting df_rc_targets
    """
    return subset_columns(df_rc_targets, cols_rc_targets).drop_duplicates()


def preprocess_approved_links(df_approved_links, cols_approved_links, article_id):
    """Function is used for preprocessing resource center approved links file.

    Keyword arguments:
    df_approved_links -- pandas dataframe for resouce center approved articles
    cols_approved_links -- columns used for subsetting df_approved_links
    """
    df_approved_links = subset_columns(df_approved_links, cols_approved_links)
    df_approved_links = combine_keys(df_approved_links, article_id, 1, False)
    return df_approved_links.drop_duplicates()


def preprocess_rc_metadata(df_rc_metadata, cols_rc_metadata, rc_content_column_name, article_id):
    """Function is used for preprocessing resource center metadata file.

    Keyword arguments:
    df_rc_metadata -- pandas dataframe for resouce center metadata.
    cols_rc_metadata -- columns used for subsetting df_rc_metadata.
    rc_content_column_name -- name of the column which contains content of rc articles.
    article_id -- name of the column which has article's id.
    """

    df_rc_metadata = subset_columns(df_rc_metadata, cols_rc_metadata)
    df_rc_metadata = remove_null_content(df_rc_metadata, rc_content_column_name)
    df_rc_metadata = combine_keys(df_rc_metadata, article_id, 1, False)
    return df_rc_metadata.drop_duplicates()


def preprocess_activity(df_activity, cols_activity, article_id, rc_flag, user_id, timestamp, activity_type):
    """Function will preprocess activity file.

    Keyword arguments:
    df_activity -- pandas dataframe for daily activity file.
    cols_activity -- columns used for subsetting df_activity.
    article_id -- name of the column which has article's id.
    rc_flag -- name of the column which contains boolean specifying if the article is resource
                center or not.
    user_id -- name of the column containing user id.
    timestamp -- name of the column containing timestamp of activity.
    activity_type -- name of the column containing activity type.

    Assumptions:
        Here we took into consideration only those activity which are coming
        from Omniture-Page View,Sailthru-Clicks and Doubleclick-Click.
        We drops the duplicate activity where he/she clicks more than once on an article
        in a day.
        Activity with tbid 0 are removed.
    """

    df_activity = subset_columns(df_activity, cols_activity)
    # fill nulls in rc flag with 0
    df_activity[rc_flag] = df_activity[rc_flag].fillna(0).astype('int')
    # drop rows with tbid or muid is null
    df_activity = df_activity.dropna(subset=[article_id, user_id])
    # sunset only click and pageview type activity
    df_activity = df_activity[df_activity[
        activity_type].isin(['Click', 'Clicks', 'Page View'])]
    # remove activity with tbid 0
    df_activity = df_activity[df_activity[article_id] != 0]
    # convert timestamp to datetime
    df_activity[timestamp] = pd.to_datetime(df_activity[timestamp])
    df_activity['timestamp_temp'] = df_activity[timestamp].dt.date
    # drop duplicates
    df_activity = df_activity.drop_duplicates(
        subset=[article_id, user_id, 'timestamp_temp'])
    df_activity = df_activity.drop('timestamp_temp', axis=1)
    # combining keys
    df_activity = combine_keys(df_activity, article_id, rc_flag, True)
    df_activity = df_activity.drop(activity_type, axis=1)
    return df_activity


# def remove_old_activity(path_file,timestamp_column,sepr = '|'):
#     '''This function will remove activities which are more than 3 months old'''
#     activity = pd.read_csv(path_activity,sep=sepr)
#     activity['temp_timestamp'] = pd.to_datetime(activity[timestamp_column])
#     last_date = dt.datetime.today() - dt.timedelta(days = 90)
#     #filter activity less than 3 months old
#     activity = activity[activity['temp_timestamp']>=last_date]
#     activity = activity.drop('temp_timestamp',axis= 1)
#     #save the dataframe
#     activity.to_csv(path_file, sep=sepr,
#                          index=False, header=True)

def preprocess_feedback(feedback_df, cols_feedback, article_id, eventdttm_feedback):
    '''This function is used to preprocess daily feedback file

    Keyword arguments:
    feedback_df -- pandas dataframe for onsite feedback file.
    cols_feedback -- columns used for subsetting feedback_df
    article_id -- name of the column which has article's id.
    eventdttm_feedback -- name of the column containing timestamp of the event

    Assumptions:
    feedback files contained some tbids which were non integers and some values
    which were string representation of integer(example '1235')
    here str_int_tbids are tbids which are string representation of integers
    string_tbids are all tbids which were not integers.
    '''

    # remove fake tbids
    tbids = feedback_df[article_id].unique().tolist()
    int_tbid = get_integer_tbids(tbids)
    string_tbid = list(set(tbids) - set(int_tbid))
    str_int_tbids = get_str_int_tbids(string_tbid)
    fake_tbids = list(set(string_tbid) - set(str_int_tbids))
    feedback_df = feedback_df[~feedback_df[article_id].isin(fake_tbids)]
    # convert tbid to int
    feedback_df[article_id] = feedback_df[article_id].astype(int)
    # combine keys
    feedback_df = combine_keys(feedback_df, article_id, 0, False)
    # subset the columns
    feedback_df = subset_columns(feedback_df, cols_feedback)
    return feedback_df


##########################################################################


def main():
    '''Wrapper function for data preprocessing. We load all files to be preprocessed
    as pandas dataframes. The dataframes are preprocessed and save back to specified
    location.
    '''

    # load files
    # df_metadata = pd.read_csv(path_metadata_dailyfeeds, sep=sepr)
    # df_metadata = pd.read_csv(path_metadata_dailyfeeds, sep=sepr)
    # df_demo = pd.read_csv(path_demo_dailyfeeds, sep=sepr)
    # df_rc_targets = pd.read_csv(path_rc_targets_dailyfeeds, sep=sepr)
    # df_approved_links = pd.read_csv(path_approved_links_dailyfeeds, sep=sepr)
    # df_rc_metadata = pd.read_csv(path_rc_metadata_dailyfeeds, sep=sepr)

    df_metadata = load_file_and_select_columns(path_metadata_dailyfeeds, sepr)
    df_demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr)
    df_rc_targets = load_file_and_select_columns(path_rc_targets_dailyfeeds, sepr)
    df_approved_links = load_file_and_select_columns(path_approved_links_dailyfeeds, sepr)
    df_rc_metadata = load_file_and_select_columns(path_rc_metadata_dailyfeeds, sepr)
    # preprocess files
    # preprocess non rc metadata
    if (len(df_metadata) != 0):
        df_metadata = preprocess_metadata(df_metadata, cols_metadata, text_column_name, article_id)
        df_metadata.to_csv(path_metadata, sep=sepr, index=False)
    else:
        print 'Error: non rc metadata was empty'
    # preprocess demo
    if (len(df_demo) != 0):
        df_demo = preprocess_demo(df_demo, cols_demo)
        df_demo.to_csv(path_demo, sep=sepr, index=False)
    else:
        print 'Error: demo was empty'
    # preprocess rc targets
    if (len(df_rc_targets) != 0):
        df_rc_targets = preprocess_rc_targets(df_rc_targets, cols_rc_targets)
        df_rc_targets.to_csv(path_rc_targets, sep=sepr, index=False)
    else:
        print 'Error: rc targets file was empty'
    # preprocess rc approved links
    if (len(df_approved_links) != 0):
        df_approved_links = preprocess_approved_links(
            df_approved_links, cols_approved_links, article_id)
        df_approved_links.to_csv(path_approved_links, sep=sepr, index=False)
    else:
        print 'Error: approved links file was empty'
    # preprocess rc metadata
    if (len(df_rc_metadata) != 0):
        df_rc_metadata = preprocess_rc_metadata(
            df_rc_metadata, cols_rc_metadata, rc_content_column_name, article_id)
        df_rc_metadata.to_csv(path_rc_metadata, sep=sepr, index=False)
    else:
        print 'Error: rc metadata was empty'


if __name__ == '__main__':
    main()
