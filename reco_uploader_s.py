#!/home/ubuntu/anaconda/bin/ipython
'''This script is used to send recommendations to the bucket(S3)
We send 3 recommendations files:
    1. Resouce center recommendations
    2. Non resource center recommendations(graph,content similarity and random)
    3. Non resouce center specialty recommendations
'''

import os
# import graph_score_matrix_for_deployement as gsm
import datetime as dt
import sys
from config_s import *
from utils_s import *
from reco_validator_utils_s import *


def copy_files(path_old, path_new):
    """This function will copy files from one path to another.

    Keyword arguments:
    path_old -- from location(string)
    path_new -- to location(string)
    """

    cmd = 'cp ' + path_old + ' ' + path_new
    if os.system(cmd) != 0:
        print 'Error : unable to move/rename file.' + path_old


def compress_files(path):
    """This function will compress file as gzip.

    Keyword arguments:
    path -- path of the file to be compressed.
    """

    cmd = 'gzip ' + path
    if os.system(cmd) != 0:
        print 'Error: unable to compress file.' + path


def upload_to_s3(path_on_instance, path_on_s3):
    """This function will upload files to s3.

    Keyword arguments:
    path_on_instance -- path of the file on instance
    path_on_s3 -- path on the bucket(s3).
    """

    cmd = '/home/ubuntu/anaconda/bin/aws s3 cp ' + path_on_instance + \
          ' ' + 's3://' + bucket_name + path_on_s3
    if os.system(cmd) != 0:
        print 'Error: unable to upload file to s3.' + path_on_instance


def upload_to_s3_2(path_on_instance, bucket_name, path_on_s3):
    """This function will upload files to s3.

    Keyword arguments:
    path_on_instance -- path of the file on instance
    path_on_s3 -- path on the bucket(s3).
    """

    cmd = '/home/ubuntu/anaconda/bin/aws s3 cp ' + path_on_instance + \
          ' ' + 's3://' + bucket_name + path_on_s3
    if os.system(cmd) != 0:
        print 'Error: unable to upload file to s3.' + path_on_instance


def upload_from_s3_to_s3(s3_path_1, s3_path_2):
    """This function will copy file from one path in s3 to another path in s3.

    Keyword arguments:
    s3_path_1 -- from path in s3
    s3_path_2 -- to path in s3
    """

    cmd = '/home/ubuntu/anaconda/bin/aws s3 cp ' + 's3://' + bucket_name + s3_path_1 + \
          ' ' + 's3://' + bucket_name + s3_path_2
    if os.system(cmd) != 0:
        print 'Error: unable to copy file from s3 to s3 with following command.' + cmd


def upload_from_s3_to_s3_2(s3_path_1, bucket_name, s3_path_2):
    """This function will copy file from one path in s3 to another path in s3.

    Keyword arguments:
    s3_path_1 -- from path in s3
    s3_path_2 -- to path in s3
    """

    cmd = '/home/ubuntu/anaconda/bin/aws s3 cp ' + 's3://' + bucket_name + s3_path_1 + \
          ' ' + 's3://' + bucket_name + s3_path_2
    if os.system(cmd) != 0:
        print 'Error: unable to copy file from s3 to s3 with following command.' + cmd


def delete_file(path):
    """This function will delete file.

    Keyword arguments:
    path -- path of the file to be deleted.
    """

    cmd = 'rm ' + path
    if os.system(cmd) != 0:
        print 'Error : unable to delete file at' + path


def upload_previous_day_reco():
    '''This function will upload previous day recommendation before the process runs.'''
    # get current and previous dates
    # bxu updates
    # current_date = dt.datetime.today().strftime('%Y%m%d')
    current_date = (dt.date.today() - dt.timedelta(delta_today)).strftime('%Y%m%d')
    # bxu updates
    # previous_date = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
    previous_date = ((dt.date.today() - dt.timedelta(delta_today)) - dt.timedelta(days=1)).strftime('%Y%m%d')

    # upload non rc recommendaiton
    previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + previous_date + \
                                          '/non_rc_recommendations_' + previous_date + '.csv'
    today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
                                   '/non_rc_recommendations_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_non_rc_reco_path_on_s3, today_non_rc_reco_path_on_s3)
    print 'uploaded last day non rc reco'
    # upload rc recommendation
    previous_day_rc_reco_path_on_s3 = rc_reco_path_on_s3 + previous_date + \
                                      '/rc_recommendations_' + previous_date + '.csv'
    today_rc_reco_path_on_s3 = rc_reco_path_on_s3 + current_date + \
                               '/rc_recommendations_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_rc_reco_path_on_s3, today_rc_reco_path_on_s3)
    print 'uploaded last day  rc reco'
    # upload specialty recommendation
    previous_day_specialty_reco_path_on_s3 = specialty_reco_path_on_s3 + previous_date + \
                                             '/specialty_recos_' + previous_date + '.csv'
    today_specialty_reco_path_on_s3 = specialty_reco_path_on_s3 + current_date + \
                                      '/specialty_recos_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_specialty_reco_path_on_s3, today_specialty_reco_path_on_s3)
    print 'uploaded last day specialty reco'


def upload_previous_day_reco2():
    '''This function will upload previous day recommendation before the process runs.'''
    # get current and previous dates
    # bxu updates
    # current_date = dt.datetime.today().strftime('%Y%m%d')
    current_date = (dt.date.today() - dt.timedelta(delta_today)).strftime('%Y%m%d')
    # bxu updates
    # previous_date = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
    previous_date = ((dt.date.today() - dt.timedelta(delta_today)) - dt.timedelta(days=1)).strftime('%Y%m%d')

    # upload non rc recommendaiton
    # bxu updates - commented
    # previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + previous_date + \
    #    '/non_rc_recommendations_' + previous_date + '.csv'
    # bxu updates - added
    previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_s + previous_date + \
                                          '/non_rc_recommendations_s_' + previous_date + '.csv'
    # bxu updates - commented
    # today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
    #    '/non_rc_recommendations_' + current_date + '.csv'
    # bxu updates - added
    today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_s + current_date + \
                                   '/non_rc_recommendations_s_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_non_rc_reco_path_on_s3, today_non_rc_reco_path_on_s3)
    print 'uploaded last day non rc reco'

    # bxu updates - all added
    previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_s + previous_date + \
                                          '/non_rc_recommendations_sc_' + previous_date + '.csv'
    today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_s + current_date + \
                                   '/non_rc_recommendations_sc_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_non_rc_reco_path_on_s3, today_non_rc_reco_path_on_s3)
    print 'uploaded last day non rc reco'

    # bxu updates - commented
    # previous_day_rc_reco_path_on_s3 = rc_reco_path_on_s3 + previous_date + \
    #    '/rc_recommendations_' + previous_date + '.csv'
    # bxu updates - added
    previous_day_rc_reco_path_on_s3 = rc_reco_path_on_s3_s + previous_date + \
                                      '/rc_recommendations_s_' + previous_date + '.csv'
    # bxu updates - commented
    # today_rc_reco_path_on_s3 = rc_reco_path_on_s3 + current_date + \
    #    '/rc_recommendations_' + current_date + '.csv'
    # bxu updates - added
    today_rc_reco_path_on_s3 = rc_reco_path_on_s3_s + current_date + \
                               '/rc_recommendations_s_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_rc_reco_path_on_s3, today_rc_reco_path_on_s3)
    print 'uploaded last day  rc reco'

    # upload specialty recommendation
    # bxu updates - all commented
    # previous_day_specialty_reco_path_on_s3 = specialty_reco_path_on_s3 + previous_date + \
    #    '/specialty_recos_' + previous_date + '.csv'
    # today_specialty_reco_path_on_s3 = specialty_reco_path_on_s3 + current_date + \
    #    '/specialty_recos_' + current_date + '.csv'
    # upload_from_s3_to_s3(previous_day_specialty_reco_path_on_s3,today_specialty_reco_path_on_s3)
    # print 'uploaded last day specialty reco'


def upload_previous_day_reco3():
    '''This function will upload previous day recommendation before the process runs.'''
    # get current and previous dates
    # bxu updates
    # current_date = dt.datetime.today().strftime('%Y%m%d')
    current_date = (dt.date.today() - dt.timedelta(delta_today)).strftime('%Y%m%d')
    # bxu updates
    # previous_date = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
    previous_date = ((dt.date.today() - dt.timedelta(delta_today)) - dt.timedelta(days=1)).strftime('%Y%m%d')

    # upload non rc recommendaiton
    # bxu updates - commented
    # previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + previous_date + \
    #    '/non_rc_recommendations_' + previous_date + '.csv'
    # bxu updates - added
    previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_s + previous_date + \
                                          '/non_rc_recommendations_s_' + previous_date + '.csv'
    # bxu updates - commented
    # today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
    #    '/non_rc_recommendations_' + current_date + '.csv'
    # bxu updates - added
    today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_s + current_date + \
                                   '/non_rc_recommendations_s_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_non_rc_reco_path_on_s3, today_non_rc_reco_path_on_s3)
    print 'uploaded last day non rc reco'

    previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_sum + previous_date + \
                                          '/specialty_recos_summary_' + previous_date + '.csv'
    # bxu updates - commented
    # today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
    #    '/non_rc_recommendations_' + current_date + '.csv'
    # bxu updates - added
    today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_s + current_date + \
                                   '/specialty_recos_summary_' + current_date + '.csv'
    upload_from_s3_to_s3(previous_day_non_rc_reco_path_on_s3, today_non_rc_reco_path_on_s3)
    print 'uploaded last day non rc reco'

    previous_day_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_tsv + previous_date + \
                                          '/mpt_recommendations_' + previous_date + '.tsv'
    # bxu updates - commented
    # today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_tsv + current_date + \
    #    '/non_rc_recommendations_' + current_date + '.tsv'
    # bxu updates - added
    today_non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3_tsv + current_date + \
                                   '/mpt_recommendations_' + current_date + '.tsv'
    upload_from_s3_to_s3_2(previous_day_non_rc_reco_path_on_s3, mpt_bucket_name, today_non_rc_reco_path_on_s3)
    print 'uploaded last day non rc reco'


def reco_uploader_wrapper(all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc,
                          all_fine_for_sending_reco_specialty):
    '''Wrapper function to upload recommendations'''

    # bxu updates - commented
    # current_date = dt.datetime.today().strftime('%Y%m%d')
    # bxu updates - added
    current_date = (dt.date.today() - dt.timedelta(delta_today)).strftime('%Y%m%d')

    # to rename files
    global non_rc_reco_path_renamed, non_rc_reco_path_on_s3, rc_reco_path_renamed, rc_reco_path_on_s3, rc_reco_path_on_s3, specialty_reco_path_renamed, specialty_reco_path_on_s3
    # non rc recommendation
    try:
        if (all_fine_for_sending_reco_non_rc):
            # copying non rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # non_rc_reco_path_renamed = non_rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_renamed_s = non_rc_reco_path_renamed_s + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(non_rc_reco_path, non_rc_reco_path_renamed)
            # bxu updates - added
            copy_files(non_rc_reco_path_s, non_rc_reco_path_renamed_s)

            # bxu updates - commented
            # non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
            #    '/non_rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_on_s3_s = non_rc_reco_path_on_s3_s + current_date + \
                                       '/non_rc_recommendations_' + current_date + '.csv'

            # bxu updates - commented
            # upload_to_s3(non_rc_reco_path_renamed, non_rc_reco_path_on_s3)
            # bxu updates - added
            upload_to_s3(non_rc_reco_path_renamed_s, non_rc_reco_path_on_s3_s)

            # bxu updates - commented
            # delete_file(non_rc_reco_path_renamed)
            # bxu updates - added
            delete_file(non_rc_reco_path_renamed_s)

            print 'upload finished for non rc recos'
        else:
            send_mail('New non rc recommendations not uploaded', attach=False)
    except Exception as e:
        print e
        send_mail('there was some error in sending non rc recommendations', attach=False)
    # rc recommendation
    try:
        if (all_fine_for_sending_reco_rc):
            # copying rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # rc_reco_path_renamed = rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            rc_reco_path_renamed_s = rc_reco_path_renamed_s + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(rc_reco_path, rc_reco_path_renamed)
            # bxu updates - added
            copy_files(rc_reco_path_s, rc_reco_path_renamed_s)

            # bxu updates - commented
            # rc_reco_path_on_s3 = rc_reco_path_on_s3 + current_date + \
            #    '/rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            rc_reco_path_on_s3_s = rc_reco_path_on_s3_s + current_date + \
                                   '/rc_recommendations_' + current_date + '.csv'

            # bxu updates - commented
            # upload_to_s3(rc_reco_path_renamed, rc_reco_path_on_s3)
            # bxu updates - added
            upload_to_s3(rc_reco_path_renamed_s, rc_reco_path_on_s3_s)

            # bxu updates - commented
            # delete_file(rc_reco_path_renamed)
            # bxu updates - added
            delete_file(rc_reco_path_renamed_s)

            print 'upload finished for rc recos'
        else:
            send_mail('New rc recommendations not uploaded', attach=False)
    except Exception as e:
        print e
        send_mail('there was some error in sending rc recommendations', attach=False)

    # specialty recommendation
    try:
        # bxu updates - commended all
        # if(all_fine_for_sending_reco_specialty):
        # copying specialty reco file to new file(with date appended in file name) and uploading
        # specialty_reco_path_renamed = specialty_reco_path_renamed + '_' + current_date + '.csv'
        # copy_files(specialty_reco_path, specialty_reco_path_renamed)
        # specialty_reco_path_on_s3 = specialty_reco_path_on_s3 + \
        #    current_date + '/specialty_recos_' + current_date + '.csv'
        # upload_to_s3(specialty_reco_path_renamed, specialty_reco_path_on_s3)
        # delete_file(specialty_reco_path_renamed)
        # print 'upload finished for specialty recos'
        # else:
        # send_mail('New specialty recommendations not uploaded',attach=False)
        # bxu updates - added
        pass
    except Exception as e:
        print e
        send_mail('there was some error in sending specialty recommendations', attach=False)


def reco_uploader_wrapper2(all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc,
                           all_fine_for_sending_reco_specialty, all_fine_for_sending_reco_non_rc_sc):
    '''Wrapper function to upload recommendations'''

    # bxu updates - commented
    # current_date = dt.datetime.today().strftime('%Y%m%d')
    # bxu updates - added
    current_date = (dt.date.today() - dt.timedelta(delta_today)).strftime('%Y%m%d')

    # to rename files
    # bxu updates - commented
    # global non_rc_reco_path_renamed, non_rc_reco_path_on_s3, rc_reco_path_renamed, rc_reco_path_on_s3, rc_reco_path_on_s3, specialty_reco_path_renamed, specialty_reco_path_on_s3
    # bxu updates - added
    global non_rc_reco_path_renamed_s, non_rc_reco_path_renamed_sc, non_rc_reco_path_on_s3_s, rc_reco_path_renamed_s, rc_reco_path_on_s3_s, rc_reco_path_on_s3_s
    # non rc recommendation
    try:
        if (all_fine_for_sending_reco_non_rc):
            # copying non rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # non_rc_reco_path_renamed = non_rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_renamed_s = non_rc_reco_path_renamed_s + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(non_rc_reco_path, non_rc_reco_path_renamed)
            # bxu updates - added
            copy_files(non_rc_reco_path_s, non_rc_reco_path_renamed_s)

            # bxu updates - commented
            # non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
            #    '/non_rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_on_s3_s = non_rc_reco_path_on_s3_s + current_date + \
                                       '/non_rc_recommendations_s_' + current_date + '.csv'

            # bxu updates - commented
            # upload_to_s3(non_rc_reco_path_renamed, non_rc_reco_path_on_s3)
            # bxu updates - added
            upload_to_s3(non_rc_reco_path_renamed_s, non_rc_reco_path_on_s3_s)

            # bxu updates - commented
            # delete_file(non_rc_reco_path_renamed)
            # bxu updates - added
            delete_file(non_rc_reco_path_renamed_s)

            print 'upload finished for non rc recos'
        else:
            send_mail('New non rc recommendations not uploaded', attach=False)
    except Exception as e:
        print e
        send_mail('there was some error in sending non rc recommendations', attach=False)

    # non rc recommendation - new schema
    try:
        if (all_fine_for_sending_reco_non_rc_sc):
            # copying non rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # non_rc_reco_path_renamed = non_rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_renamed_sc = non_rc_reco_path_renamed_sc + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(non_rc_reco_path, non_rc_reco_path_renamed)
            # bxu updates - added
            copy_files(non_rc_reco_path_sc, non_rc_reco_path_renamed_sc)

            # bxu updates - commented
            # non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
            #    '/non_rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_on_s3_s = non_rc_reco_path_on_s3_s + current_date + \
                                       '/non_rc_recommendations_sc_' + current_date + '.csv'

            # bxu updates - commented
            # upload_to_s3(non_rc_reco_path_renamed, non_rc_reco_path_on_s3)
            # bxu updates - added
            upload_to_s3(non_rc_reco_path_renamed_sc, non_rc_reco_path_on_s3_s)

            # bxu updates - commented
            # delete_file(non_rc_reco_path_renamed)
            # bxu updates - added
            delete_file(non_rc_reco_path_renamed_sc)

            print 'upload finished for non rc recos with new schema'
        else:
            send_mail('New non rc recommendations not uploaded', attach=False)
    except Exception as e:
        print e
        send_mail('there was some error in sending non rc recommendations', attach=False)
    # rc recommendation
    try:
        if (all_fine_for_sending_reco_rc):
            # copying rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # rc_reco_path_renamed = rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            rc_reco_path_renamed_s = rc_reco_path_renamed_s + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(rc_reco_path, rc_reco_path_renamed)
            # bxu updates - added
            copy_files(rc_reco_path, rc_reco_path_renamed_s)

            # bxu updates - commented
            # rc_reco_path_on_s3 = rc_reco_path_on_s3 + current_date + \
            #    '/rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            rc_reco_path_on_s3_s = rc_reco_path_on_s3_s + current_date + \
                                   '/rc_recommendations_s_' + current_date + '.csv'

            # bxu updates - commented
            # upload_to_s3(rc_reco_path_renamed, rc_reco_path_on_s3)
            # bxu updates - added
            upload_to_s3(rc_reco_path_renamed_s, rc_reco_path_on_s3_s)

            # bxu updates - commented
            # delete_file(rc_reco_path_renamed)
            # bxu updates - added
            delete_file(rc_reco_path_renamed_s)

            print 'upload finished for rc recos'
        else:
            send_mail('New rc recommendations not uploaded', attach=False)
    except Exception as e:
        print e
        send_mail('there was some error in sending rc recommendations', attach=False)

    # specialty recommendation
    try:
        # bxu updates - commended all
        # if(all_fine_for_sending_reco_specialty):
        # copying specialty reco file to new file(with date appended in file name) and uploading
        # specialty_reco_path_renamed = specialty_reco_path_renamed + '_' + current_date + '.csv'
        # copy_files(specialty_reco_path, specialty_reco_path_renamed)
        # specialty_reco_path_on_s3 = specialty_reco_path_on_s3 + \
        #    current_date + '/specialty_recos_' + current_date + '.csv'
        # upload_to_s3(specialty_reco_path_renamed, specialty_reco_path_on_s3)
        # delete_file(specialty_reco_path_renamed)
        # print 'upload finished for specialty recos'
        # else:
        # send_mail('New specialty recommendations not uploaded',attach=False)
        # bxu updates - added
        pass
    except Exception as e:
        print e
        send_mail('there was some error in sending specialty recommendations', attach=False)


def reco_uploader_wrapper3():
    '''Wrapper function to upload recommendations'''

    # bxu updates - commented
    # current_date = dt.datetime.today().strftime('%Y%m%d')
    # bxu updates - added
    current_date = (dt.date.today() - dt.timedelta(delta_today)).strftime('%Y%m%d')

    # to rename files
    # bxu updates - commented
    # global non_rc_reco_path_renamed, non_rc_reco_path_on_s3, rc_reco_path_renamed, rc_reco_path_on_s3, rc_reco_path_on_s3, specialty_reco_path_renamed, specialty_reco_path_on_s3
    # bxu updates - added
    global non_rc_reco_path_on_s3_tsv, non_rc_reco_path_renamed_tsv, non_rc_reco_path_renamed_s, non_rc_reco_path_renamed_sc, non_rc_reco_path_on_s3_s, rc_reco_path_renamed_s, rc_reco_path_on_s3_s, rc_reco_path_on_s3_s

    global non_rc_reco_path_renamed_sum, non_rc_reco_path_on_s3_sum, mpt_bucket_name
    # non rc recommendation
    try:
        if (True):
            # copying non rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # non_rc_reco_path_renamed = non_rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_renamed_s = non_rc_reco_path_renamed_s + '_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_renamed_sum = non_rc_reco_path_renamed_sum + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(non_rc_reco_path, non_rc_reco_path_renamed)
            # bxu updates - added
            copy_files(non_rc_reco_path_s, non_rc_reco_path_renamed_s)
            copy_files(non_rc_recos_path_sum, non_rc_reco_path_renamed_sum)

            # bxu updates - commented
            # non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
            #    '/non_rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_on_s3_s = non_rc_reco_path_on_s3_s + current_date + \
                                       '/non_rc_recommendations_s_' + current_date + '.csv'
            # bxu updates - added
            # non_rc_reco_path_on_s3_s = non_rc_reco_path_on_s3_s + current_date + \
            #    '/non_rc_recommendations_s.csv'
            # bxu updates - added
            non_rc_reco_path_on_s3_sum = non_rc_reco_path_on_s3_sum + current_date + \
                                         '/specialty_recos_summary_' + current_date + '.csv'

            # bxu updates - commented
            # upload_to_s3(non_rc_reco_path_renamed, non_rc_reco_path_on_s3)
            # bxu updates - added
            upload_to_s3(non_rc_reco_path_renamed_s, non_rc_reco_path_on_s3_s)
            # bxu updates - added
            upload_to_s3(non_rc_reco_path_renamed_sum, non_rc_reco_path_on_s3_sum)

            # bxu updates - commented
            # delete_file(non_rc_reco_path_renamed)
            # bxu updates - added
            delete_file(non_rc_reco_path_renamed_s)
            # bxu updates - added
            delete_file(non_rc_reco_path_renamed_sum)

            print 'upload finished for non rc recos'
        else:
            # send_mail('New non rc recommendations not uploaded',attach=False)
            pass
    except Exception as e:
        print e
        # send_mail('there was some error in sending non rc recommendations',attach=False)
        print 'there was some error in sending non rc recommendations'

    try:
        if (False):  # S3 target folder changed
            # copying non rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # non_rc_reco_path_renamed = non_rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_renamed_tsv = non_rc_reco_path_renamed_tsv + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(non_rc_reco_path, non_rc_reco_path_renamed)
            # bxu updates - added
            copy_files(non_rc_reco_path_tsv, non_rc_reco_path_renamed_tsv)

            # bxu updates - commented
            # non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
            #    '/non_rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_on_s3_tsv = non_rc_reco_path_on_s3_tsv + current_date + \
                                         '/mpt_recommendations_' + current_date + '.tsv'

            # bxu updates - commented
            # upload_to_s3(non_rc_reco_path_renamed, non_rc_reco_path_on_s3)
            # bxu updates - added
            upload_to_s3(non_rc_reco_path_renamed_tsv, non_rc_reco_path_on_s3_tsv)

            # bxu updates - commented
            # delete_file(non_rc_reco_path_renamed)
            # bxu updates - added
            # delete_file(non_rc_reco_path_renamed_tsv)

            print 'upload finished for non rc recos'
        else:
            # send_mail('New non rc recommendations not uploaded',attach=False)
            pass
    except Exception as e:
        print e
        # send_mail('there was some error in sending non rc recommendations in tsv format',attach=False)
        print 'there was some error in sending non rc recommendations in tsv format'
    try:
        if (True):  # New tsv S3 target folder
            # copying non rc reco file to new file(with date appended in file name) and uploading
            # bxu updates - commented
            # non_rc_reco_path_renamed = non_rc_reco_path_renamed + '_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_renamed_tsv = non_rc_reco_path_renamed_tsv + '_' + current_date + '.csv'

            # bxu updates - commented
            # copy_files(non_rc_reco_path, non_rc_reco_path_renamed)
            # bxu updates - added
            copy_files(non_rc_reco_path_tsv, non_rc_reco_path_renamed_tsv)

            # bxu updates - commented
            # non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
            #    '/non_rc_recommendations_' + current_date + '.csv'
            # bxu updates - added
            non_rc_reco_path_on_s3_tsv = non_rc_reco_path_on_s3_tsv + current_date + \
                                         '/mpt_recommendations_' + current_date + '.tsv'

            # bxu updates - commented
            # upload_to_s3(non_rc_reco_path_renamed, non_rc_reco_path_on_s3)
            # bxu updates - added
            # upload_to_s3(non_rc_reco_path_renamed_tsv, non_rc_reco_path_on_s3_tsv)
            upload_to_s3_2(non_rc_reco_path_renamed_tsv, mpt_bucket_name, non_rc_reco_path_on_s3_tsv)

            # bxu updates - commented
            # delete_file(non_rc_reco_path_renamed)
            # bxu updates - added
            delete_file(non_rc_reco_path_renamed_tsv)

            print 'upload finished for non rc recos'
        else:
            # send_mail('New non rc recommendations not uploaded',attach=False)
            pass
    except Exception as e:
        print e
        # send_mail('there was some error in sending non rc recommendations in tsv format',attach=False)
        print 'there was some error in sending non rc recommendations in tsv format'


if __name__ == '__main__':
    # call wrapper function
    try:
        # bxu updates - commented
        # upload_previous_day_reco()
        # bxu updates - added
        # upload_previous_day_reco2()
        # bxu updates - added
        upload_previous_day_reco3()
    except Exception as e:
        print e
        # send_mail('Previous date recommendations not uploaded')
        print 'Previous date recommendations not uploaded'
