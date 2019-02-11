#!/home/ubuntu/anaconda/bin/ipython
"""Script will generate recommendations for non rc medium active users."""
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
    dh_activity, _, non_rc_approved_articles = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)
    # graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list = user_list_loader()
    _, _, content_reco_user_list, _ = user_list_loader()
    # load similarity score files
    # similarity_score_df = spark.read.csv(
    #     path_content_score_matrix, header=True, inferSchema=True, sep=sepr)
    similarity_score_df = load_file_and_select_columns(path_content_score_matrix, sepr, spark=spark)
    cattopic_score_df = load_file_and_select_columns(path_cattopic_score, sepr=sepr, spark=spark)
    score_file = similarity_score_df.join(cattopic_score_df, on=[id_x, id_y], how='left')
    score_column = [ss_score_col, col_cattopic_score]
    # Generate recommendation for non rc users
    non_rc_content_recos = generate_non_rc_recos(spark, dh_activity, content_reco_user_list, non_rc_approved_articles,
                                                 score_file, user_id, article_id, id_x, id_y, score_column,
                                                 rank_col=rank_col, reco_type="c")
    del similarity_score_df, dh_activity, score_file
    # combine csv files
    pyspark_df_to_disk(non_rc_content_recos, temp_non_rc_recos_path,
                       sepr_recos, append_mode=True)


if __name__ == '__main__':
    main()
