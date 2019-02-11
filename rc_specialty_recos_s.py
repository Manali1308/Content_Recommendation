# imports
import pandas as pd
import random
from pyspark.sql.window import Window
# from graph_score_matrix_for_deployement import load_files_and_clean
from config_s import *
from utils_s import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from recommendation_generation_engine import *

# from graph_score_generator import file_loader
##########################################################################
# spark configuration
conf = SparkConf()
conf.setMaster(
    "local[*]").setAppName('App Name').set("spark.executor.memory", "100g").set("spark.driver.memory", "100g").set(
    "spark.result.maxResultSize", "100g")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.master("local[*]").appName("App Name").config(
    "spark.some.config.option", "some-value", conf).getOrCreate()
print "importing and initialisation finished"


##########################################################################

def find_missing_users(spark, temp_rc_recos_path, sepr_recos, reco_schema, user_id, df_rc_targets, sepr):
    """Function will find missed users from recos.

    Keyword arguments:
    spark -- spark object.
    temp_rc_recos_path -- path of the folder where parts of rc recommendatins are saved(string).
    sepr_recos -- seperator for reco file.
    reco_schema -- schema for reco file.
    user_id -- name of the column conatining user id(string).
    df_rc_targets -- pyspark dataframe for rc target users and advertiser id mapping.
    sepr -- default seperator(string).
    """

    # rc_recos = spark.read.csv(
    #     temp_rc_recos_path, sep=sepr_recos, schema=reco_schema)
    rc_recos = load_file_and_select_columns(temp_rc_recos_path, sepr_recos, spark=spark, schema=reco_schema)
    recos_given_user_list = make_unique_col_list(rc_recos, user_id)
    del rc_recos
    all_user_list = make_unique_col_list(df_rc_targets, user_id)
    return list(set(all_user_list) - set(recos_given_user_list))


def main():
    # load activity file and relevant article list
    dh_activity, _, _ = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)
    # load demo file
    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    # generate reco user lists
    # all_user_list = make_unique_col_list(dh_demo, user_id)
    # graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list = user_list_loader()
    # bxu updates - commented
    # _, _, _, speciality_reco_user_list = user_list_loader()
    # bxu updates - added
    speciality_reco_user_list_all = load_list_from_pickle(path_speciality_reco_user_list_all)
    # load resource centre files
    df_approved_links = load_file_and_select_columns(
        path_approved_links, sepr, cols_approved_links, spark)
    df_rc_targets = load_file_and_select_columns(
        path_rc_targets, sepr, cols_rc_targets, spark)

    # generate rc specailty recommendation
    # bxu updates - commented
    # rc_specialty_recos = generate_rc_specialty_recos(
    #    spark, dh_activity, df_rc_targets, speciality_reco_user_list, df_approved_links, user_id, article_id, rank_col, advertiser_id,reco_flag=specialty_based)
    rc_specialty_recos = generate_rc_specialty_recos(
        spark, dh_activity, df_rc_targets, speciality_reco_user_list_all, df_approved_links, user_id, article_id,
        rank_col, advertiser_id, reco_flag=specialty_based)
    # bxu updates - commented
    # pyspark_df_to_disk(rc_specialty_recos, temp_rc_recos_path,
    #                   sepr_recos, append_mode=True)
    # bxu updates - added
    pyspark_df_to_disk(rc_specialty_recos, temp_rc_recos_path_s,
                       sepr_recos, append_mode=False)
    # find left out rc target users

    missing_user_list = find_missing_users(
        spark, temp_rc_recos_path_s, sepr_recos, reco_schema, user_id, df_rc_targets, sepr)
    # bxu question: 	Do I need to

    # bxu updates - commented
    # rc_missed_recos = generate_rc_specialty_recos(
    #    spark, dh_activity, df_rc_targets, missing_user_list, df_approved_links, user_id, article_id, rank_col, advertiser_id,reco_flag=specialty_based)
    # bxu updates - commented
    # pyspark_df_to_disk(rc_missed_recos, temp_rc_recos_path,
    #                   sepr_recos, append_mode=True)
    # combine files
    # bxu updates - commented
    # combine_pyspark_files(temp_rc_recos_path, rc_reco_path)
    # bxu updates - added
    combine_pyspark_files(temp_rc_recos_path_s, rc_reco_path_s)


if __name__ == '__main__':
    main()
