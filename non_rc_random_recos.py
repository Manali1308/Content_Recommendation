#!/home/ubuntu/anaconda/bin/ipython
"""Script will generate recommendations for randomly chosed pct_random(as defined in config) percent of high active non rc users."""
##########################################################################
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


def main():
    # load activity file and relevant article list
    dh_activity, relevant_urlid, non_rc_approved_articles = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)
    # load demo file
    dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    # graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list = user_list_loader()
    _, graph_random_user_list, _, _ = user_list_loader()
    # find non rc random users
    non_rc_random_users = list(graph_random_user_list)
    # non_rc_random_users.extend(content_random_user_list)
    non_rc_random_recos = non_rc_specialty_recommendations(spark, dh_activity, dh_demo, non_rc_approved_articles,
                                                           non_rc_max_reco, \
                                                           article_id, user_id, non_rc_random_users, rank_col,
                                                           random_based)

    # non_rc_random_recos = spark.createDataFrame(non_rc_random_recos)
    # save to disk
    pyspark_df_to_disk(non_rc_random_recos,
                       temp_non_rc_recos_path, sepr_recos, append_mode=True)
    # combine csv files
    combine_pyspark_files(temp_non_rc_recos_path, non_rc_reco_path)


if __name__ == '__main__':
    main()
