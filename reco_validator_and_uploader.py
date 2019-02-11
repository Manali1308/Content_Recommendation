#!/home/ubuntu/anaconda/bin/ipython
"""This script will check if recommendation generated are valid or not. If hard checks are passed, the recos will be sent.
Mail alert will be sent if checks fail."""

import smtplib
import os
import datetime as dt
import sys
from config import *
from utils import *
from reco_validator_utils import *
import pyspark.sql.functions as F
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
from pyspark.sql.window import Window
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import subprocess
import reco_uploader

# from graph_score_generator import file_loader
##########################################################################
# spark configuration
spark = get_spark_session_object()


def check_if_reco_files_are_valid(non_rc_reco, rc_reco, specialty_recos, rc_lookback, non_rc_lookback, demo, df_rc_appr,
                                  non_rc_meta_approved, df_rc_targets):
    '''This wrapper function will check if the reco files are are valid.
    The functions used in this functions are imported from reco_validator_utils'''

    # apply checks
    # check if reco file have necessary columns
    try:
        check_1_rc, check_1_non_rc, check_1_specialty = check_if_reco_columns_as_expected(rc_reco, non_rc_reco,
                                                                                          specialty_recos,
                                                                                          expected_reco_col_list)
        print('check_if_reco_columns_as_expected done')
    except Exception as e:
        print e
        send_mail('check_if_reco_columns_as_expected failed')
    # check if reco types in reco file are as expected
    try:
        check_2_rc, check_2_non_rc, check_2_specialty = check_reco_types_in_reco_file(rc_reco, non_rc_reco,
                                                                                      specialty_recos,
                                                                                      expected_reco_flag_in_rc_reco,
                                                                                      expected_reco_flag_in_non_rc_reco,
                                                                                      expected_reco_flag_in_specialty_reco)
        print('check_reco_types_in_reco_file done')
    except Exception as e:
        print e
        send_mail('check_reco_types_in_reco_file failed')
    # check if article ids in reco files belong to approved articles
    try:
        check_3_rc, check_3_non_rc, check_3_specialty = check_if_article_ids_are_from_metadata(df_rc_appr,
                                                                                               non_rc_meta_approved,
                                                                                               rc_reco, non_rc_reco,
                                                                                               specialty_recos,
                                                                                               article_id)
        print('check_if_article_ids_are_from_metadata done')
    except Exception as e:
        print e
        send_mail('check_if_article_ids_are_from_metadata failed')
    # check if activities in past 2 months are removed from reco
    try:
        check_4 = check_if_lookback_recos_are_removed(non_rc_reco, rc_reco, non_rc_lookback, rc_lookback, user_id,
                                                      article_id)
        print('check_if_lookback_recos_are_removed done')
    except Exception as e:
        print e
        send_mail('check_if_lookback_recos_are_removed failed')
    # check if no rc recommendation is sent more than 5 times
    try:
        check_6_rc, check_6_non_rc, check_6_specialty = check_if_userid_in_reco_are_present_in_demo_file(rc_reco,
                                                                                                         non_rc_reco,
                                                                                                         specialty_recos,
                                                                                                         demo,
                                                                                                         df_rc_targets)
        print('check_if_userid_in_reco_are_present_in_demo_file done')
    except Exception as e:
        print e
        send_mail('check_if_userid_in_reco_are_present_in_demo_file failed')
    # check the number of recos per user
    try:
        check_number_of_recos_per_user(non_rc_reco, no_of_recommendations, user_id, article_id)
        print('check_number_of_recos_per_user done')
    except Exception as e:
        print e
        send_mail('check_number_of_recos_per_user failed')
    # check if reco type is consistent with the type of user(for eg. graph users are send graph recommendation)
    try:
        check_reco_type_and_user_type_are_consistent(rc_reco, non_rc_reco, path_graph_reco_user_list \
                                                     , path_graph_random_user_list, path_content_reco_user_list)
        print('check_reco_type_and_user_type_are_consistent done')
    except Exception as e:
        print e
        send_mail('check_reco_type_and_user_type_are_consistent failed')
    # check if all users are sent reco
    try:
        check_if_all_users_are_sent_reco(rc_reco, non_rc_reco, specialty_recos, path_rc_targets, path_demo)
        print('check_if_all_users_are_sent_reco check done')
    except Exception as e:
        print e
        send_mail('check_if_all_users_are_sent_reco check failed')
    try:
        check_number_of_reco_type_per_user_is_one(rc_reco, non_rc_reco, specialty_recos)
        print('check for user being sent reco on only 1 reco type done')
    except Exception as e:
        print e
        send_mail('check_number_of_reco_type_per_user_is_one check failed')
    # return true if hard checks pass
    return (check_1_rc & check_2_rc & check_3_rc & check_6_rc), (
            check_1_non_rc & check_2_non_rc & check_3_non_rc & check_6_non_rc), (
                   check_1_specialty & check_2_specialty & check_3_specialty & check_6_specialty)


def main():
    all_fine_for_sending_reco = True

    non_rc_reco = load_file_and_select_columns(non_rc_reco_path, sepr_recos)
    rc_reco = load_file_and_select_columns(rc_reco_path, sepr_recos)
    specialty_recos = load_file_and_select_columns(path_recos_for_specialty_based_users, sepr_recos)
    rc_lookback = load_file_and_select_columns(path_rc_lookback_file, sepr)
    non_rc_lookback = load_file_and_select_columns(path_non_rc_lookback_file, sepr)
    # remove trailing 0/1 from tbids
    rc_lookback = detach_keys(rc_lookback, article_id)
    non_rc_lookback = detach_keys(non_rc_lookback, article_id)
    # load other files
    demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr)
    df_rc_appr = load_file_and_select_columns(path_approved_links_dailyfeeds, sepr=sepr)
    df_rc_targets = load_file_and_select_columns(path_rc_targets, sepr=sepr)
    non_rc_meta = load_file_and_select_columns(path_metadata_dailyfeeds, sepr=sepr)
    non_rc_meta_approved = non_rc_meta[non_rc_meta[approved_bool] == True]
    # analysis of reco
    analysis_of_reco(rc_reco, non_rc_reco, specialty_recos)
    print 'analysis done'
    # check if recos are valid
    all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc, all_fine_for_sending_reco_specialty = check_if_reco_files_are_valid(
        non_rc_reco, rc_reco, specialty_recos, rc_lookback, non_rc_lookback, demo, df_rc_appr, non_rc_meta_approved,
        df_rc_targets)
    # check if score matrix are valid
    check_if_score_matrices_are_valid(spark, path_score_matrix, path_content_score_matrix, path_approved_links,
                                      cols_approved_links
                                      , path_metadata, cols_metadata, sepr, approved_bool, article_id, sepr_recos,
                                      ss_score_col)

    try:
        is_error_present, error_text = error_in_log()
        if is_error_present:
            send_mail('There was some error in log' + '\n' + error_text, attach=False)
    except:
        print send_mail('log error checking failed', attach=False)
    # send recos if generated recos pass the checks
    try:
        reco_uploader.reco_uploader_wrapper(all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc,
                                            all_fine_for_sending_reco_specialty)
    except Exception as e:
        print e
        send_mail('Some error in uploading new recommendation', attach=False)


if __name__ == '__main__':
    main()
