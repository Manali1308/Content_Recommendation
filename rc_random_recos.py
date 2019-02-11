#!/home/ubuntu/anaconda/bin/ipython
"""Script will generate recommendations for common users between Resource center target users and the randomly chosen high active users."""
# imports
import pandas as pd
import random
from pyspark.sql.window import Window
# from graph_score_matrix_for_deployement import load_files_and_clean
from config import *
from utils import *
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


def main():
    # load activity file and relevant article list
    dh_activity, _, _ = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)

    # graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list = user_list_loader()
    _, graph_random_user_list, _, _ = user_list_loader()
    # load resource centre files
    df_approved_links = load_file_and_select_columns(
        path_approved_links, sepr, cols_approved_links, spark)
    df_rc_targets = load_file_and_select_columns(
        path_rc_targets, sepr, cols_rc_targets, spark)
    rc_random_user_list = list(graph_random_user_list)
    # rc_random_user_list.extend(content_random_user_list)
    # rc_random_user_list.extend(specialty_random_user_list)
    # random recos for missing users
    # rc_random_user_list.extend(missing_user_list)

    # rc_random_recos = generate_rc_random_recos(spark,
    #     article_id, df_approved_links, rc_random_user_list, df_rc_targets, user_id, rank_col, reco_flag = random_based)

    rc_random_recos = generate_rc_specialty_recos(
        spark, dh_activity, df_rc_targets, rc_random_user_list, df_approved_links, user_id, article_id, rank_col,
        advertiser_id, reco_flag=random_based)

    pyspark_df_to_disk(rc_random_recos, temp_rc_recos_path,
                       sepr_recos, append_mode=True)


if __name__ == '__main__':
    main()
