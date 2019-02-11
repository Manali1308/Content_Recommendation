#!/home/ubuntu/anaconda/bin/ipython
"""Script will generate recommendations for non rc high active users."""
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


##########################################################################
# user list generator block


def main():
    # load activity file and relevant article list
    dh_activity, _, non_rc_approved_articles = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)
    # graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list = user_list_loader()
    graph_reco_user_list, _, _, _ = user_list_loader()
    # load similarity score files
    # graph_score_df = spark.read.csv(
    #     path_score_matrix, header=True, inferSchema=True, sep=sepr_graph_matrix)
    graph_score_df = load_file_and_select_columns(path_score_matrix, sepr_graph_matrix, spark=spark)
    # non rc reco gen
    score_column = ['score_pair']
    non_rc_graph_recos = generate_non_rc_recos(spark, dh_activity, graph_reco_user_list, non_rc_approved_articles,
                                               graph_score_df, user_id, article_id, id_x, id_y, score_column,
                                               rank_col=rank_col, reco_type="g")

    pyspark_df_to_disk(non_rc_graph_recos, temp_non_rc_recos_path,
                       sepr_recos, append_mode=False)


if __name__ == '__main__':
    main()
