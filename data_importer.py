#!/home/ubuntu/anaconda/bin/ipython
"""Script will import the data flat files from s3 to instance daily. 
    Files which are used after incrementation:
        mpt_activity, mpt_DisplayedOnsiteFeedback
    Files which are used after replacement:
        mpt_ResourceCenter_Metadata, mpt_content_metadata, mpt_demo, mpt_approvedlinks, mpt_ResourceCenter_Targets
"""

##########################################################################
from config import *
import os
# import pickle
import pandas as pd
from utils import *
from datetime import datetime, timedelta
import time
from data_preprocessor import *
import numpy as np
from reco_validator_utils import send_mail


##########################################################################
# function


def copy_files(bucket_name, path_on_s3, path_on_instance):
    """Function will copy data files(tar.gz file) from s3 to instance.

     Keyword arguments:
     bucket_name -- the name of the bucket where data is present(string)
     path_on_s3 --  path of the file on the bucket(string)
     path_on_instance --  the path on instance where the file will be downloaded.(string)

    Output: Boolean(True/False) specifying whether copy happened or not
    """
    cmd = '/home/ubuntu/anaconda/bin/aws s3 cp s3://' + \
          bucket_name + path_on_s3 + ' ' + path_on_instance

    copied = False
    # get cut off time to keep pinging for data
    cut_off_time = datetime.today().strftime('%Y-%m-%d') + ' ' + cut_off_data_pinging_time
    cut_off_time = pd.to_datetime(cut_off_time)
    # while file is not copied or time is less than cut_of_pinging time
    while True:
        if os.system(cmd) == 0:
            copied = True
        elif datetime.today() < cut_off_time:
            print 'sleeping now'
            time.sleep(60)
        if (copied) | (datetime.today() > cut_off_time):
            break

    if not copied:
        print 'unable to sync file' + path_on_instance
        send_mail(path_on_s3 + ' : S3 path did not have file at ' + cut_off_data_pinging_time + ' UTC',
                  toaddr=eh_kf_mail_list, attach=False)
        return False
    else:
        print 'copied file' + path_on_s3
        return True


def untar_files(tar_path, untar_folder_location):
    """Function will untar files.

    Keyword arguments:
    tar_path -- path of the tar file(string)
    untar_folder_location -- path of the folder in which the untarred file will be saved(string)

    Output: Boolean(True/False) specifying whether untarring happened or not
    Assumption: The file to be untarred is in .tar.gz/.tar format
    """

    cmd = 'tar -xzvf' + ' ' + tar_path + ' ' + '-C' + ' ' + untar_folder_location
    if os.system(cmd) != 0:
        # will decompress it to same location.
        # for .gz compression
        cmd = 'gunzip' + ' ' + tar_path
        if os.system(cmd) != 0:
            print 'unable to untar file' + untar_folder_location
            return False
    else:
        return True


def increase_date(date_stamp):
    """Take date as input, increase date by 1 day and return the incremented date in YYYYMMDD integer format.

    Keyword arguments:
    date_stamp -- date(integer)

    Output: incremented date(integer).
    """

    date_stamp = datetime.strptime(str(date_stamp), '%Y%m%d')
    return int((date_stamp + timedelta(1)).strftime('%Y%m%d'))


def load_list_of_dates(path_list, start_date):
    '''This function will load the list of dates on which data importing happened and create a new list with start_date if list
    doesn't exist.

    Keyword arguments:
    path_list -- path of the list(saved as pickle) to be loaded.(string)
    start_date -- date to initiate the list in case the list doesn't exist at path_list.(integer)

    Output: list of dates on which data importing happened
    '''

    try:
        dates_list = load_list_from_pickle(path_list)
    except Exception as e:
        print e
        # create a list if IOError occurs which has err no 2
        if e.errno == 2:
            dates_list = [start_date]
    return dates_list


def sync(path_list, path_dailyfeeds_on_s3, name_file_on_s3, path_on_instance, path_dailyfeeds_on_instance):
    """Function will copy files,untar it,and modify data importing dates list.
    
    Keyword arguments:
    path_list -- path of the list(saved as pickle) on which data importing happened.(string)
    path_dailyfeeds_on_s3 -- path on the bucket which contains data folders by date.(string)
    name_file_on_s3 -- name of the file to be copied on the bucket.(string)
    path_on_instance -- path on instance where the data will be copied to.(string)
    path_dailyfeeds_on_instance -- folder location where all daily-imported files are saved.(string)
    """

    global start_date
    # load list of dates on which syncing happened
    dates_list = load_list_of_dates(path_list, start_date)
    # get latest date uptill when sync has happened
    max_date = np.max(dates_list)
    print 'starting after ' + name_file_on_s3 + ' ' + str(max_date)
    # continue importing till today's date
    continue_importing = find_today_date() > max_date
    while (continue_importing):
        max_date = increase_date(max_date)
        path_on_s3 = path_dailyfeeds_on_s3 + str(max_date) + name_file_on_s3
        if copy_files(bucket_name, path_on_s3, path_on_instance):
            if untar_files(path_on_instance, path_dailyfeeds_on_instance):
                dates_list.append(max_date)
                print 'copied for' + str(max_date)
        # check if max date is equal to today's date
        if max_date >= find_today_date():
            continue_importing = False
    print 'latest available date for ' + path_on_instance + 'is ' + str(np.max(dates_list))
    save_list_as_pickle(dates_list, path_list)


def main_activity_sync(path_list_activity_dates, bucket_name, path_dailyfeeds_on_s3, \
                       path_activity_dailyfeeds, path_activity, path_dailyfeeds_on_instance, \
                       name_activity_on_s3, path_activity_zip_on_instance):
    '''This function will copy activity files, untar it, append to the accumulated activity file after preprocessing.

    Keyword arguments:
    path_list_activity_dates -- path of the list(saved as pickle) which contains list of dates on which activity file was imported.(string)
    bucket_name -- name of the bucket.(string)
    path_dailyfeeds_on_s3 -- path on the bucket which contains data folders by date.(string)
    path_activity_dailyfeeds -- path on which untarred activity file is saved.(string)
    path_activity -- path in store_inputs folder where the daily activity will be appended after preprocessing.(string)
    path_dailyfeeds_on_instance -- folder location where all daily-imported files are saved.(string)
    name_activity_on_s3 -- name of the activity file on the bucket.(string)
    path_activity_zip_on_instance -- path where zipped activity file will be copied to.(string)
    '''

    global start_date
    # load list of dates on which syncing happened
    list_activity_dates = load_list_of_dates(path_list_activity_dates, start_date)
    # get last date on which sync happened
    max_date = np.max(list_activity_dates)
    # continue importing till today's date
    continue_importing = find_today_date() > max_date
    while (continue_importing):
        # increasing max date
        max_date = increase_date(max_date)
        path_activity_on_s3 = path_dailyfeeds_on_s3 + \
                              str(max_date) + name_activity_on_s3
        if copy_files(bucket_name, path_activity_on_s3, path_activity_zip_on_instance):
            print 'copied'
            if untar_files(path_activity_zip_on_instance, path_dailyfeeds_on_instance):
                print 'untarred'
                if len(list_activity_dates) == 1:  # if this is start date iteration
                    append_activity(path_activity_dailyfeeds, path_activity, head=True)
                else:
                    append_activity(path_activity_dailyfeeds, path_activity, head=False)
                list_activity_dates.append(max_date)
        # check if max date is equal to today's date
        if max_date >= find_today_date():
            continue_importing = False
    print 'latest available date for activity file is ' + str(np.max(list_activity_dates))
    # remove the activity which are more than 3 months old
    # remove_old_activity(path_activity,timestamp,sepr=sepr)
    # save the syncing-date list again
    save_list_as_pickle(list_activity_dates, path_list_activity_dates)


def feedback_data_sync(path_list_feedback_date, path_dailyfeeds_on_s3, name_feedback_on_s3, \
                       bucket_name, path_feedback_zip_on_instance, path_dailyfeeds_on_instance, \
                       path_feedback_dailyfeeds, path_feedback):
    '''This function will copy feedback file, untar it and append to accumulated feedback file.

    Keyword arguments:
    path_list_feedback_date -- path of the list(saved as pickle) which contains list of dates on which feedback file was imported.(string)
    path_dailyfeeds_on_s3 -- path on the bucket which contains data folders by date.(string)
    name_feedback_on_s3 -- name of the feedback file on the bucket.(string)
    bucket_name -- name of the bucket.(string)
    path_feedback_zip_on_instance -- path where zipped feedback file will be copied to.(string)
    path_dailyfeeds_on_instance -- folder location where all daily-imported files are saved.(string)
    path_feedback_dailyfeeds -- path on which untarred feedback file is saved.(string)
    path_feedback -- path in store_inputs folder where the daily feedback will be appended after preprocessing.(string)
    '''

    global start_date
    # load list of dates on which syncing happened
    list_feedback_dates = load_list_of_dates(path_list_feedback_date, start_date_feedback)
    # get last date on which sync happened
    max_date = np.max(list_feedback_dates)
    # continue importing till today's date
    continue_importing = find_today_date() > max_date
    while (continue_importing):
        # increase max_date
        max_date = increase_date(max_date)
        path_feedback_on_s3 = path_dailyfeeds_on_s3 + str(max_date) + name_feedback_on_s3
        if copy_files(bucket_name, path_feedback_on_s3, path_feedback_zip_on_instance):
            print 'copied'
            if untar_files(path_feedback_zip_on_instance, path_dailyfeeds_on_instance):
                print 'untarred'
                #                 append_send(path_send_dailyfeeds, path_send)
                if np.max(list_feedback_dates) == start_date_feedback:
                    append_feedback(path_feedback_dailyfeeds, path_feedback, to_append=False)
                else:
                    append_feedback(path_feedback_dailyfeeds, path_feedback, to_append=True)
                list_feedback_dates.append(max_date)
        # check if max date is equal to today's date
        if max_date >= find_today_date():
            continue_importing = False
    print 'latest available date for feedback file is ' + str(np.max(list_feedback_dates))
    save_list_as_pickle(list_feedback_dates, path_list_feedback_date)


def append_activity(path_activity_dailyfeeds, path_activity, head=False):
    '''This function will load the daily activity file and append it to the acccumulated activity file after preprocessing.
    
    Keyword arguments:
    path_activity_dailyfeeds -- file location of the untarred daily activity file.(string)
    path_activity -- file location where daily activity file will be replaced/appended after preprocessing.(string) 
    head -- Boolean(True/False) specifying whether to replace/append the daily activity.
    '''

    # reading daily activity file
    activity_file = load_file_and_select_columns(path_activity_dailyfeeds, sepr)  # load activity file
    # preprocessing
    activity_file = preprocess_activity(
        activity_file, cols_activity, article_id, rc_flag, user_id, timestamp, activity_type)
    # preprocess daily activity
    activity_file[article_id] = activity_file[article_id].astype('int')  # type cast article id to int
    activity_file[user_id] = activity_file[user_id].astype('int')  # type cast user id to int
    # saving to csv
    if head:
        activity_file.to_csv(path_activity, sep=sepr,
                             index=False, header=True)
    else:
        activity_file.to_csv(path_activity, sep=sepr,
                             index=False, mode='a', header=False)  # append daily activity to overall activity


def append_feedback(path_feedback_dailyfeeds, path_feedback, to_append=True):
    '''This function will load the daily activity file and append it to the acccumulated activity file after preprocessing.

    Keyword arguments:
    path_feedback_dailyfeeds -- file location of the untarred daily feedback file.(string)
    path_feedback -- file location where daily feedback file will be replaced/appended after preprocessing.(string) 
    to_append -- Boolean(True/False) specifying whether to append/replace the daily feedback.
    '''

    # load the dailyfeeds feedback file.
    feedback_df = load_file_and_select_columns(path_feedback_dailyfeeds, sepr_feedback)
    # preprocessing
    feedback_df = preprocess_feedback(
        feedback_df, cols_feedback, article_id, eventdttm_feedback)
    # preprocess daily send
    feedback_df[article_id] = feedback_df[article_id].astype('int')  # type cast article id to int
    # saving to csv
    if to_append:
        feedback_df.to_csv(path_feedback, sep=sepr, index=False, mode='a',
                           header=False)  # append daily activity to overall activity
    else:
        feedback_df.to_csv(path_feedback, sep=sepr, index=False)


##########################################################################


def main():
    """This functio will import all files."""
    try:
        main_activity_sync(path_list_activity_dates, bucket_name, path_dailyfeeds_on_s3,
                           path_activity_dailyfeeds, path_activity, path_dailyfeeds_on_instance,
                           name_activity_on_s3, path_activity_zip_on_instance)
        print 'main_activity_sync_completed'
    except Exception as e:
        print e  # print the exception if occurs

    try:
        # for metadata
        sync(path_list_metadata_dates, path_dailyfeeds_on_s3,
             name_metadata_on_s3, path_metadata_zip_on_instance, path_dailyfeeds_on_instance)
        print 'sync complete for metadata'
    except Exception as e:
        print e

    try:
        # for demo
        sync(path_list_demo_dates, path_dailyfeeds_on_s3,
             name_demo_on_s3, path_demo_zip_on_instance, path_dailyfeeds_on_instance)
        print 'sync complete for demo'
    except Exception as e:
        print e

    try:
        # for RC content metadata
        sync(path_list_rc_metadata_dates, path_dailyfeeds_on_s3,
             name_rc_metadata_on_s3, path_rc_metadata_zip_on_instance, path_dailyfeeds_on_instance)
        print 'sync complete for rc metadata'
    except Exception as e:
        print e

    try:
        # for RC approved links
        sync(path_list_rc_approved_links_dates, path_dailyfeeds_on_s3,
             name_rc_approved_links_on_s3, path_rc_approved_links_zip_on_instance, path_dailyfeeds_on_instance)
        print 'sync complete for rc approved links'
    except Exception as e:
        print e

    try:
        # for RC targets
        sync(path_list_rc_targets_dates, path_dailyfeeds_on_s3,
             name_rc_targets_on_s3, path_rc_targets_zip_on_instance, path_dailyfeeds_on_instance)
        print 'sync complete for rc targets'
    except Exception as e:
        print e

    try:
        # for feedback data
        feedback_data_sync(path_list_feedback_date, path_dailyfeeds_on_s3, name_feedback_on_s3, bucket_name,
                           path_feedback_zip_on_instance, path_dailyfeeds_on_instance, \
                           path_feedback_dailyfeeds, path_feedback)
        print 'feedback data sync'
    except Exception as e:
        print e  # print the exception if occurs


if __name__ == '__main__':
    # call main function
    main()
