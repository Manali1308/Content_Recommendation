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
from config import *
from utils import *
from reco_validator_utils import *


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
    current_date = dt.datetime.today().strftime(
        '%Y%m%d')
    previous_date = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
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


def reco_uploader_wrapper(all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc,
                          all_fine_for_sending_reco_specialty):
    '''Wrapper function to upload recommendations'''

    current_date = dt.datetime.today().strftime(
        '%Y%m%d')  # present date in str format
    # to rename files
    global non_rc_reco_path_renamed, non_rc_reco_path_on_s3, rc_reco_path_renamed, rc_reco_path_on_s3, rc_reco_path_on_s3, specialty_reco_path_renamed, specialty_reco_path_on_s3
    # non rc recommendation
    try:
        if (all_fine_for_sending_reco_non_rc):
            # copying non rc reco file to new file(with date appended in file name) and uploading
            non_rc_reco_path_renamed = non_rc_reco_path_renamed + '_' + current_date + '.csv'
            copy_files(non_rc_reco_path, non_rc_reco_path_renamed)
            non_rc_reco_path_on_s3 = non_rc_reco_path_on_s3 + current_date + \
                                     '/non_rc_recommendations_' + current_date + '.csv'
            upload_to_s3(non_rc_reco_path_renamed, non_rc_reco_path_on_s3)
            delete_file(non_rc_reco_path_renamed)
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
            rc_reco_path_renamed = rc_reco_path_renamed + '_' + current_date + '.csv'
            copy_files(rc_reco_path, rc_reco_path_renamed)
            rc_reco_path_on_s3 = rc_reco_path_on_s3 + current_date + \
                                 '/rc_recommendations_' + current_date + '.csv'
            upload_to_s3(rc_reco_path_renamed, rc_reco_path_on_s3)
            delete_file(rc_reco_path_renamed)
            print 'upload finished for rc recos'
        else:
            send_mail('New rc recommendations not uploaded', attach=False)
    except Exception as e:
        print e
        send_mail('there was some error in sending rc recommendations', attach=False)
    # specialty recommendation
    try:
        if (all_fine_for_sending_reco_specialty):
            # copying specialty reco file to new file(with date appended in file name) and uploading
            specialty_reco_path_renamed = specialty_reco_path_renamed + '_' + current_date + '.csv'
            copy_files(specialty_reco_path, specialty_reco_path_renamed)
            specialty_reco_path_on_s3 = specialty_reco_path_on_s3 + \
                                        current_date + '/specialty_recos_' + current_date + '.csv'
            upload_to_s3(specialty_reco_path_renamed, specialty_reco_path_on_s3)
            delete_file(specialty_reco_path_renamed)
            print 'upload finished for specialty recos'
        else:
            send_mail('New specialty recommendations not uploaded', attach=False)
    except Exception as e:
        print e
        send_mail('there was some error in sending specialty recommendations', attach=False)


if __name__ == '__main__':
    # call wrapper function
    # this will close all the jupyter notebooks befor the process starts
    # try:
    #    os.system("PIFS=$IFS; IFS=$'\n'; for i in `jupyter notebook list  2> /dev/null | grep http`; do   j=`echo $i| tr -cd '[[:digit:]]'`; k=`lsof -n -i4TCP:$j | grep -m 1 'jupyter'| awk -F ' ' '{printf $2}' `; kill -15 $k; done ; IFS=$PIFS")
    # except Exception as e:
    #    print e
    try:
        upload_previous_day_reco()
    except Exception as e:
        print e
        send_mail('Previous date recommendations not uploaded')
