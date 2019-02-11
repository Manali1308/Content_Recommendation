"""This script will check if recommendation generated are valid or not. If hard checks are passed, the recos will be sent.
Mail alert will be sent if checks fail."""

import smtplib
import os
import datetime as dt
import sys
from config_s import *
from utils_s import *
from reco_validator_utils_s import *
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
# bxu updates - commented
import reco_uploader_s
# bxu updates - added
# from reco_uploader_s import *
# bxu updates - added
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id, row_number

from recommendation_generation_engine_s import find_specialty_missed_users

from postprocessing_s import *

from reco_validator_utils_s import *

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


def check_if_reco_files_are_valid2(non_rc_reco, rc_reco, specialty_recos, rc_lookback, non_rc_lookback, demo,
                                   df_rc_appr, non_rc_meta_approved, df_rc_targets):
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
        # bxu updates - commented
        # check_reco_type_and_user_type_are_consistent(rc_reco,non_rc_reco,path_graph_reco_user_list \
        #                                            ,path_graph_random_user_list,path_content_reco_user_list)
        # print('check_reco_type_and_user_type_are_consistent done')
        # bxu updates - added
        pass
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


def add_new_schema(non_rc_reco_path_s):
    # bxu
    # Questions: Is it 'UnifiedAccountID' or 'AccountID'? here I first use 'AccountID'

    tmp_non_rc_recos_df = load_file_and_select_columns(non_rc_reco_path_s, sepr_recos, spark=spark, schema=reco_schema)
    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    dh_demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr, spark=spark)
    dh_demo = dh_demo.select(cols_demo2)
    dh_demo.drop_duplicates()
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.join(dh_demo, on=user_id, how='inner')

    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df_ns.withColumn('Specialty', lit('non-specialty'))
    tmp_non_rc_recos_df_s = tmp_non_rc_recos_df.filter(~tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df = tmp_non_rc_recos_df_s.union(tmp_non_rc_recos_df_ns)

    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("site_property", lit('MPT'))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("ID_type", lit('TBID'))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("model_version", lit('P7-14-90'))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("reco_model", lit('PSR'))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('AccountID', 'account_id')
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('TBID', 'ID')
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('Specialty', 'Reason')

    # add "account_id",
    return tmp_non_rc_recos_df.select(cols_non_rc_reco_sc)


def reformat_non_rc_ori(non_rc_reco_path_s):
    # bxu
    # Questions: Is it 'UnifiedAccountID' or 'AccountID'? here I first use 'AccountID'

    tmp_non_rc_recos_df = load_file_and_select_columns(non_rc_reco_path_s, sepr_recos, spark=spark, schema=reco_schema)
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("TBID", tmp_non_rc_recos_df["TBID"].cast(StringType()))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.groupBy("MasterUserID").agg(concat_ws(', ', collect_list("TBID")))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed("concat_ws(, , collect_list(TBID))", 'tbid')

    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    dh_demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr, spark=spark)
    dh_demo = dh_demo.select(['MasterUserID', 'AccountID', 'Specialty'])
    dh_demo.drop_duplicates()
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.join(dh_demo, on=user_id, how='inner')

    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df_ns.limit(1).withColumn('MasterUserID', lit('999999999'))

    tmp_non_rc_recos_df_s = tmp_non_rc_recos_df.filter(~tmp_non_rc_recos_df.Specialty.isNull())
    # tmp_non_rc_recos_df=tmp_non_rc_recos_df_s.union(tmp_non_rc_recos_df_ns)
    tmp_non_rc_recos_df = tmp_non_rc_recos_df_ns.union(tmp_non_rc_recos_df_s)

    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('AccountID', 'mpt_id')
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('MasterUserID', 'muid')

    return tmp_non_rc_recos_df.select(['muid', 'mpt_id', 'tbid'])


def duplicate(testList, n):
    y = 0
    x = len(testList)
    newList = []
    while y < x:
        for z in range(0, n):
            newList.append(testList[y])
        y = y + 1
    return newList


def load_non_reco_file_full_ori(path_recos_for_specialty_based_users_s_bak, path_recos_for_specialty_based_users_s):
    tmp_non_rc_recos_df_bak = load_file_and_select_columns(path_recos_for_specialty_based_users_s_bak, sepr_recos,
                                                           spark=spark, schema=reco_schema)
    tmp_non_rc_recos_df = load_file_and_select_columns(path_recos_for_specialty_based_users_s, sepr_recos, spark=spark,
                                                       schema=reco_schema)

    df_count_recos_per_user = tmp_non_rc_recos_df.groupby("MasterUserID").agg({article_id: 'count'})
    df_count_recos_per_user2 = df_count_recos_per_user[df_count_recos_per_user["count(TBID)"] < 10]
    newDF = df_count_recos_per_user2.groupBy("count(TBID)").count()
    df_count_recos_per_user3 = df_count_recos_per_user[df_count_recos_per_user["count(TBID)"] >= 10]
    user_list_normal = make_unique_col_list(df_count_recos_per_user3, "MasterUserID")

    non_specialty_user_list = find_specialty_missed_users(spark, path_speciality_reco_user_list_all,
                                                          temp_non_rc_recos_path_s)
    reco_user_short = make_unique_col_list(df_count_recos_per_user2, user_id)
    # we need to add more default recos to this list
    reco_user_short.extend(non_specialty_user_list)

    user_list_for_non_specialty = load_list_from_pickle(path_non_specialty_user_list_s)
    tmp_non_specialty_non_rc = filter_by_join(spark, tmp_non_rc_recos_df_bak, user_list_for_non_specialty, user_id)

    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.sort(["MasterUserID", "rank"]).limit(
        len(reco_user_short) * no_of_recommendations)
    newDF = tmp_non_specialty_non_rc.groupBy("rank").count()

    user_list_for_non_specialty = duplicate(reco_user_short, no_of_recommendations)

    b = spark.createDataFrame([(l,) for l in user_list_for_non_specialty], ['MasterUserID2'])
    b.createOrReplaceTempView('b')
    b = spark.sql('select row_number() over (order by "MasterUserID2") as num, * from b')

    tmp_non_specialty_non_rc.createOrReplaceTempView('tmp_non_specialty_non_rc')
    tmp_non_specialty_non_rc = spark.sql(
        'select row_number() over (order by "MasterUserID", "rank") as num, * from tmp_non_specialty_non_rc')

    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.join(b, "num", "inner").drop('num')
    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.drop('MasterUserID')

    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.withColumnRenamed("MasterUserID2", "MasterUserID")

    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.withColumn("rank", tmp_non_specialty_non_rc.rank + 10)

    tmp_non_rc_recos_df = tmp_non_rc_recos_df.union(tmp_non_specialty_non_rc)

    non_rc_lookback_df = load_file_and_select_columns(path_non_rc_lookback_file, sepr, spark=spark,
                                                      schema=restriction_schema)
    tmp_non_rc_recos_df = removed_already_viewed_articles(spark, tmp_non_rc_recos_df, non_rc_lookback_df, user_id,
                                                          article_id)

    tmp_non_rc_recos_df = rerank(tmp_non_rc_recos_df, user_id, rank_col)
    tmp_non_rc_recos_df = take_fewer_recos(tmp_non_rc_recos_df, n_non_cc_reco10, rank_col)

    df_count_recos_per_user = tmp_non_rc_recos_df.groupby("MasterUserID").agg({article_id: 'count'})
    df_count_recos_per_user = df_count_recos_per_user[df_count_recos_per_user["count(TBID)"] == 10]
    reco_user_list = make_unique_col_list(df_count_recos_per_user, user_id)
    tmp_non_rc_recos_df = filter_by_join(spark, tmp_non_rc_recos_df, reco_user_list, user_id)

    # df_count_recos_per_user2.count()
    # 186,533

    # tmp_non_rc_recos_df = detach_keys(tmp_non_rc_recos_df, article_id)
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.sort(['MasterUserID', 'rank'])
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("TBID", tmp_non_rc_recos_df["TBID"].cast(StringType()))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.groupBy("MasterUserID").agg(concat_ws(', ', collect_list("TBID")))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed("concat_ws(, , collect_list(TBID))", 'tbid')

    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    dh_demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr, spark=spark)
    dh_demo = dh_demo.select(['MasterUserID', 'AccountID', 'Specialty'])
    dh_demo.drop_duplicates()
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.join(dh_demo, on=user_id, how='inner')

    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.Specialty.isNull())
    # tmp_non_rc_recos_df_ns=tmp_non_rc_recos_df_ns.withColumn('MasterUserID', lit('999999999'))
    # <test these two> - moved to the end
    tmp_non_rc_recos_df_ns_1 = tmp_non_rc_recos_df_ns.limit(1).withColumn('AccountID', lit(''))
    tmp_non_rc_recos_df_ns_1 = tmp_non_rc_recos_df_ns_1.withColumn('MasterUserID', lit('999999999'))
    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df_ns.union(tmp_non_rc_recos_df_ns_1)

    tmp_non_rc_recos_df_s = tmp_non_rc_recos_df.filter(~tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df = tmp_non_rc_recos_df_s.union(tmp_non_rc_recos_df_ns)

    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('AccountID', 'mpt_id')
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('MasterUserID', 'muid')

    return tmp_non_rc_recos_df.select(['muid', 'mpt_id', 'tbid'])


def load_non_reco_file_full(path_recos_for_specialty_based_users_s_bak, path_recos_for_specialty_based_users_s):
    tmp_non_rc_recos_df_bak = load_file_and_select_columns(path_recos_for_specialty_based_users_s_bak, sepr_recos,
                                                           spark=spark, schema=reco_schema)
    '''
    tmp_non_rc_recos_df = load_file_and_select_columns(path_recos_for_specialty_based_users_s, sepr_recos,spark=spark,schema = reco_schema)
    df_count_recos_per_user = tmp_non_rc_recos_df.groupby("MasterUserID").agg({article_id:'count'})
    df_count_recos_per_user2 = df_count_recos_per_user[df_count_recos_per_user["count(TBID)"]<10]
    newDF = df_count_recos_per_user2.groupBy("count(TBID)").count()
    df_count_recos_per_user3 = df_count_recos_per_user[df_count_recos_per_user["count(TBID)"]>=10]
    user_list_normal = make_unique_col_list(df_count_recos_per_user3,"MasterUserID")
            
    non_specialty_user_list = find_specialty_missed_users(spark,path_speciality_reco_user_list_all,temp_non_rc_recos_path_s)
    reco_user_short = make_unique_col_list(df_count_recos_per_user2, user_id)
    #we need to add more default recos to this list
    reco_user_short.extend(non_specialty_user_list)
        
    user_list_for_non_specialty = load_list_from_pickle(path_non_specialty_user_list_s)
    tmp_non_specialty_non_rc = filter_by_join(spark, tmp_non_rc_recos_df_bak, user_list_for_non_specialty, user_id)
    
    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.sort(["MasterUserID", "rank"]).limit(len(reco_user_short)*no_of_recommendations)
    newDF = tmp_non_specialty_non_rc.groupBy("rank").count()
    
    user_list_for_non_specialty = duplicate(reco_user_short, no_of_recommendations)

    b = spark.createDataFrame([(l,) for l in user_list_for_non_specialty], ['MasterUserID2'])
    b.createOrReplaceTempView('b')
    b=spark.sql('select row_number() over (order by "MasterUserID2") as num, * from b')
    
    tmp_non_specialty_non_rc.createOrReplaceTempView('tmp_non_specialty_non_rc')
    tmp_non_specialty_non_rc=spark.sql('select row_number() over (order by "MasterUserID", "rank") as num, * from tmp_non_specialty_non_rc')
    
    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.join(b, "num", "inner").drop('num')
    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.drop('MasterUserID')
    
    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.withColumnRenamed("MasterUserID2", "MasterUserID")
    
    
    tmp_non_specialty_non_rc = tmp_non_specialty_non_rc.withColumn("rank", tmp_non_specialty_non_rc.rank+10)
    
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.union(tmp_non_specialty_non_rc)
    '''
    non_rc_lookback_df = load_file_and_select_columns(path_non_rc_lookback_file, sepr, spark=spark,
                                                      schema=restriction_schema)
    tmp_non_rc_recos_df = removed_already_viewed_articles(spark, tmp_non_rc_recos_df_bak, non_rc_lookback_df, user_id,
                                                          article_id)

    tmp_non_rc_recos_df = rerank(tmp_non_rc_recos_df, user_id, rank_col)
    tmp_non_rc_recos_df = take_fewer_recos(tmp_non_rc_recos_df, n_non_cc_reco10, rank_col)

    df_count_recos_per_user = tmp_non_rc_recos_df.groupby("MasterUserID").agg({article_id: 'count'})
    df_count_recos_per_user = df_count_recos_per_user[df_count_recos_per_user["count(TBID)"] == 10]
    reco_user_list = make_unique_col_list(df_count_recos_per_user, user_id)
    tmp_non_rc_recos_df = filter_by_join(spark, tmp_non_rc_recos_df, reco_user_list, user_id)

    # df_count_recos_per_user2.count()
    # 186,533

    # tmp_non_rc_recos_df = detach_keys(tmp_non_rc_recos_df, article_id)
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.sort(['MasterUserID', 'rank'])
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("TBID", tmp_non_rc_recos_df["TBID"].cast(StringType()))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.groupBy("MasterUserID").agg(concat_ws(', ', collect_list("TBID")))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed("concat_ws(, , collect_list(TBID))", 'tbid')

    tmp_non_rc_recos_df_bak = take_fewer_recos(tmp_non_rc_recos_df_bak, n_non_cc_reco10, rank_col)
    tmp_non_rc_recos_df_bak = tmp_non_rc_recos_df_bak.sort(['MasterUserID', 'rank'])
    tmp_non_rc_recos_df_bak = tmp_non_rc_recos_df_bak.withColumn("TBID",
                                                                 tmp_non_rc_recos_df_bak["TBID"].cast(StringType()))
    tmp_non_rc_recos_df_bak = tmp_non_rc_recos_df_bak.groupBy("MasterUserID").agg(concat_ws(', ', collect_list("TBID")))
    tmp_non_rc_recos_df_bak = tmp_non_rc_recos_df_bak.withColumnRenamed("concat_ws(, , collect_list(TBID))", 'tbid')

    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    dh_demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr, spark=spark)
    dh_demo = dh_demo.select(['MasterUserID', 'AccountID', 'Specialty'])
    dh_demo.drop_duplicates()
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.join(dh_demo, on=user_id, how='inner')

    tmp_non_rc_recos_df_bak = tmp_non_rc_recos_df_bak.join(dh_demo, on=user_id, how='inner')
    tmp_non_rc_recos_df_ns_1 = tmp_non_rc_recos_df_bak.filter(tmp_non_rc_recos_df_bak.Specialty.isNull()) \
        .limit(1).withColumn('AccountID', lit('')) \
        .withColumn('MasterUserID', lit('999999999'))

    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df_ns.union(tmp_non_rc_recos_df_ns_1)

    tmp_non_rc_recos_df_s = tmp_non_rc_recos_df.filter(~tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df = tmp_non_rc_recos_df_s.union(tmp_non_rc_recos_df_ns)

    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('AccountID', 'mpt_id')
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('MasterUserID', 'muid')

    return tmp_non_rc_recos_df.select(['muid', 'mpt_id', 'tbid'])


def reformat_non_rc(non_rc_reco_path_s):
    # bxu
    # Questions: Is it 'UnifiedAccountID' or 'AccountID'? here I first use 'AccountID'

    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumn("TBID", tmp_non_rc_recos_df["TBID"].cast(StringType()))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.groupBy("MasterUserID").agg(concat_ws(', ', collect_list("TBID")))
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed("concat_ws(, , collect_list(TBID))", 'tbid')

    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    dh_demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr, spark=spark)
    dh_demo = dh_demo.select(['MasterUserID', 'AccountID', 'Specialty'])
    dh_demo.drop_duplicates()
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.join(dh_demo, on=user_id, how='inner')

    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df_ns.withColumn('MasterUserID', lit('999999999'))
    # <test these two>
    tmp_non_rc_recos_df_ns_1 = tmp_non_rc_recos_df_ns.limit(1).withColumn('AccountID', lit(''))
    # tmp_non_rc_recos_df_ns=tmp_non_rc_recos_df_ns_1.union(tmp_non_rc_recos_df_ns)

    tmp_non_rc_recos_df_s = tmp_non_rc_recos_df.filter(~tmp_non_rc_recos_df.Specialty.isNull())
    # tmp_non_rc_recos_df=tmp_non_rc_recos_df_s.union(tmp_non_rc_recos_df_ns)
    tmp_non_rc_recos_df = tmp_non_rc_recos_df_ns.union(tmp_non_rc_recos_df_s)

    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('AccountID', 'mpt_id')
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.withColumnRenamed('MasterUserID', 'muid')

    return tmp_non_rc_recos_df.select(['muid', 'mpt_id', 'tbid'])


def reco_analysis_by_specialty(path_recos_for_specialty_based_users_s_bak):
    '''input: path_recos_for_specialty_based_users_s_bak - the output of non_rc_specialty_recos_s.py
       output: the recommendations for each specialty before any removal  
    '''
    tmp_non_rc_recos_df = load_file_and_select_columns(path_recos_for_specialty_based_users_s_bak, sepr_recos,
                                                       spark=spark, schema=reco_schema)
    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    dh_demo = load_file_and_select_columns(path_demo_dailyfeeds, sepr, spark=spark)
    dh_demo = dh_demo.select(cols_demo2)
    dh_demo.drop_duplicates()
    tmp_non_rc_recos_df = tmp_non_rc_recos_df.join(dh_demo, on=user_id, how='inner')

    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df_ns = tmp_non_rc_recos_df_ns.withColumn('Specialty', lit('non-specialty'))
    tmp_non_rc_recos_df_s = tmp_non_rc_recos_df.filter(~tmp_non_rc_recos_df.Specialty.isNull())
    tmp_non_rc_recos_df = tmp_non_rc_recos_df_s.union(tmp_non_rc_recos_df_ns)

    tmp_non_rc_recos_df_2 = tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.rank == 1)
    tmp_non_rc_recos_df_3 = tmp_non_rc_recos_df_2.withColumn("row_num", row_number().over(
        Window.partitionBy(["Specialty"]).orderBy("MasterUserID")))
    tmp_non_rc_recos_df_4 = tmp_non_rc_recos_df_3.filter(tmp_non_rc_recos_df_3.row_num == 1)

    # tmp_non_rc_recos_df=tmp_non_rc_recos_df.withColumn("row_num", row_number().over(Window.partitionBy(["Specialty", "MasterUserID"]).orderBy("rank")))
    # tmp_non_rc_recos_df2=tmp_non_rc_recos_df.filter(tmp_non_rc_recos_df.row_num==1)

    reco_user_s = make_unique_col_list(tmp_non_rc_recos_df_4, user_id)
    tmp_non_rc_recos_df3 = filter_by_join(spark, tmp_non_rc_recos_df, reco_user_s, user_id)
    tmp_non_rc_recos_df4 = tmp_non_rc_recos_df3.select(['Specialty', 'TBID', 'rank']).sort(['Specialty', 'rank'])
    tmp_non_rc_recos_df4.count()

    # tmp_non_rc_recos_df4.toPandas().to_csv(non_rc_recos_path_sum)

    pyspark_df_to_disk(tmp_non_rc_recos_df4, non_rc_recos_sum_path, sepr_recos, append_mode=False)
    combine_pyspark_files(non_rc_recos_sum_path, non_rc_recos_path_sum, ['Specialty', 'TBID', 'rank'])

    return tmp_non_rc_recos_df4


def main():
    all_fine_for_sending_reco = True
    '''
    #bxu updates - commmented 
    #non_rc_reco = load_file_and_select_columns(non_rc_reco_path,sepr_recos)
    #bxu updates - added 
    non_rc_reco = load_file_and_select_columns(non_rc_reco_path_s,sepr_recos)

    #bxu updates - commmented - recovered 
    rc_reco = load_file_and_select_columns(rc_reco_path,sepr_recos)
    #bxu updates - added - commented: since we will not use our own RC recomender
    #rc_reco = load_file_and_select_columns(rc_reco_path_s,sepr_recos)

    #bxu updates - commmented 
    #specialty_recos =load_file_and_select_columns(path_recos_for_specialty_based_users,sepr_recos)
    #bxu updates - added - want to make the process work with this data
    specialty_recos =load_file_and_select_columns(non_rc_reco_path_s,sepr_recos)

    rc_lookback = load_file_and_select_columns(path_rc_lookback_file,sepr)
    non_rc_lookback = load_file_and_select_columns(path_non_rc_lookback_file,sepr)
    #remove trailing 0/1 from tbids
    rc_lookback = detach_keys(rc_lookback, article_id)
    non_rc_lookback = detach_keys(non_rc_lookback, article_id)
    #load other files
    demo = load_file_and_select_columns(path_demo_dailyfeeds,sepr)
    df_rc_appr = load_file_and_select_columns(path_approved_links_dailyfeeds,sepr = sepr)
    df_rc_targets = load_file_and_select_columns(path_rc_targets,sepr = sepr)
    non_rc_meta = load_file_and_select_columns(path_metadata_dailyfeeds,sepr = sepr)
    non_rc_meta_approved = non_rc_meta[non_rc_meta[approved_bool]==True]
    #analysis of reco

    #bxu updates - commented
    #analysis_of_reco(rc_reco,non_rc_reco,specialty_recos)
    #bxu updates - added - commented
    #analysis_of_reco2(rc_reco,non_rc_reco,specialty_recos, path_reco_analysis_s)
    #print 'analysis done'
    #check if recos are valid
    '''
    # bxu updates - commented <will be working on it later >
    # all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc,all_fine_for_sending_reco_specialty= check_if_reco_files_are_valid(non_rc_reco,rc_reco,specialty_recos,rc_lookback,non_rc_lookback,demo,df_rc_appr,non_rc_meta_approved,df_rc_targets)
    # bxu updates - added - here I first comment it: actually on the non_rc_reco is needed to be checked!
    # all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc,all_fine_for_sending_reco_specialty= check_if_reco_files_are_valid2(non_rc_reco,rc_reco,specialty_recos,rc_lookback,non_rc_lookback,demo,df_rc_appr,non_rc_meta_approved,df_rc_targets)
    # bxu updates - added
    all_fine_for_sending_reco_non_rc = True
    # bxu updates - added
    all_fine_for_sending_reco_rc = False
    # bxu updates - added
    all_fine_for_sending_reco_specialty = False
    # bxu updates - added
    all_fine_for_sending_reco_non_rc_sc = False

    # check if score matrix are valid
    # bxu updates - commented
    # check_if_score_matrices_are_valid(spark,path_score_matrix,path_content_score_matrix,path_approved_links,cols_approved_links
    # ,path_metadata, cols_metadata,sepr,approved_bool,article_id,sepr_recos,ss_score_col)

    try:
        # bxu updates - commented
        # is_error_present, error_text = error_in_log()
        # bxu updates - added
        # is_error_present, error_text = error_in_log2(log_file_path)
        # if is_error_present:
        #    send_mail('There was some error in log'+'\n'+error_text,attach=False)
        pass
    except:
        # send_mail('log error checking failed',attach=False)
        print 'log error checking failed'

    # send recos if generated recos pass the checks
    try:

        # bxu updates - added
        # non_rc_sc_df = add_new_schema(non_rc_reco_path_s)
        # bxu updates - added - for tsv format - for making exact 10 recos
        non_rc_sc_df_tsv = load_non_reco_file_full(path_recos_for_specialty_based_users_s_bak,
                                                   path_recos_for_specialty_based_users_s)
        tmp_non_rc_recos_df = reco_analysis_by_specialty(path_recos_for_specialty_based_users_s_bak)
        # bxu updates - added - for tsv format
        # non_rc_sc_df_tsv = reformat_non_rc(non_rc_reco_path_s)
        # non_rc_sc_df_tsv.write.option("delimiter", "\t").csv(non_rc_reco_path_tsv)

        # bxu updates - added - for new schema
        # pyspark_df_to_disk(
        #    non_rc_sc_df, temp_non_rc_recos_path_sc, sepr_recos, append_mode=False)
        # bxu updates - added - for tsv format
        pyspark_df_to_disk(
            non_rc_sc_df_tsv, temp_non_rc_recos_path_tsv, sepr_tsv, append_mode=False)
        # bxu updates - added - for new schema
        # combine_pyspark_files(temp_non_rc_recos_path_sc, non_rc_reco_path_sc, cols_non_rc_reco_sc)
        # bxu updates - added - tsv file
        combine_pyspark_files(temp_non_rc_recos_path_tsv, non_rc_reco_path_tsv, ['muid', 'mpt_id', 'tbid'], sepr_tsv)
        # bxu updates - commented
        reco_uploader_s.reco_uploader_wrapper3()
        # bxu updates - added
        # reco_uploader_wrapper3()
        # reco_uploader_wrapper2(all_fine_for_sending_reco_rc, all_fine_for_sending_reco_non_rc,all_fine_for_sending_reco_specialty, all_fine_for_sending_reco_non_rc_sc)
    except Exception as e:
        print e
        # bxu updates - commented
        # send_mail('Some error in uploading new recommendation',attach=False)
        # bxu updates - commented
        send_mail('Some error in adding new schemea or uploading new recommendation', attach=False)


if __name__ == '__main__':
    main()
