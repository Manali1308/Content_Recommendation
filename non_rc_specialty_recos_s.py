#!/home/ubuntu/anaconda/bin/ipython
try:
    # import graph_score_matrix_for_deployement as gsm
    # import libraries
    import pandas as pd
    from utils_s import *
    from config_s import *
    # from graph_score_matrix_for_deployement import load_files_and_clean
    # import numpy as np
    import datetime as dt
    # import tarfile
    # import re
    from pyspark.sql.window import Window
    import datetime as dt
    # import os
    import sys
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.conf import SparkConf
    import numpy as np
    from recommendation_generation_engine_s import *
except Exception as e:
    message = 'error importing libraries in file running daily'
    print message
    print e
    sys.out(1)

##########################################################################
# spark

spark = get_spark_session_object()
print "importing and initialisation finished"


##########################################################################

def main():
    # load files
    # bxu updates
    # commented by bxu
    # dh_activity, _, relevant_urlid = file_loader(
    #    spark, path_activity, cols_activity_graph, sepr,
    #    article_id, path_metadata, cols_metadata, path_approved_links,
    #    cols_approved_links, approved_bool, days_to_consider)

    # bxu updates
    # added by bxu
    dh_activity, dh_activity2, dh_activity3, _, relevant_urlid = file_loader2(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider, days_to_consider2, days_to_consider3)
    # bxu updates - commented
    # dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    # bxu updates - added
    dh_demo = load_demo2(spark, path_demo, sepr, cols_demo)

    dh_metadata = load_metadata(
        spark, path_metadata, cols_metadata, sepr)
    # bxu updates - commented
    # user_list_for_specialty_based_recos = load_list_from_pickle(
    #    path_speciality_reco_user_list)
    user_list_for_specialty_based_recos = load_list_from_pickle(
        path_speciality_reco_user_list_all)
    # append the graph-content user who missed getting graph-content reco
    try:
        # bxu updates
        # commented by bxu
        # user_missed_in_graph_content = find_graph_content_missed_users(spark,path_graph_reco_user_list,path_content_reco_user_list,temp_non_rc_recos_path)
        # user_list_for_specialty_based_recos.extend(user_missed_in_graph_content)
        pass
    except Exception as e:
        print e
    # random_user_list = load_list_from_pickle(path_specialty_random_user_list)

    # specialty_article_table = most_popular_relevant_articles_per_specialty(
    #     dh_activity, dh_demo, relevant_urlid, no_of_recommendations, article_id, user_id)
    # print 'most popular articles evaluated'
    # dh_demo = filter_by_join(
    #     spark, dh_demo, user_list_for_specialty_based_recos, user_id)
    # print 'demo files filtered for users'
    # dh_demo = dh_demo.join(specialty_article_table, on='mapped_' +
    #                        user_specialty, how='inner')
    # print 'recommendations generated'

    # del specialty_article_table
    # dh_demo = dh_demo.select([user_id, article_id, rank_col])
    # print 'colums selected'
    # dh_demo = dh_demo.withColumn('reco_flag', lit(specialty_based))
    # print 'type column added'
    # # detach keys
    # dh_demo = detach_keys(dh_demo, article_id)
    # dh_demo = dh_demo.drop_duplicates(subset=[user_id,rank_col])

    # bxu updates
    # commented by bxu
    # specialty_reco = non_rc_specialty_recommendations(spark,dh_activity, dh_demo, relevant_urlid, no_of_recommendations,\
    #                    article_id, user_id,user_list_for_specialty_based_recos,rank_col,specialty_based)

    # bxu updates
    # added by bxu
    specialty_reco = non_rc_specialty_recommendations2(spark, dh_activity, dh_activity2, dh_activity3, dh_demo,
                                                       relevant_urlid, no_of_recommendations, \
                                                       article_id, user_id, user_list_for_specialty_based_recos,
                                                       rank_col, specialty_based)

    # detach keys
    specialty_reco = detach_keys(specialty_reco, article_id)
    # save to disk
    # bxu updates - commented
    # pyspark_df_to_disk(
    #    specialty_reco, temp_path_for_specialty_reco, sepr_recos, append_mode=False)
    # bxu updates - added
    pyspark_df_to_disk(
        specialty_reco, temp_non_rc_recos_path_s, sepr_recos, append_mode=False)
    del specialty_reco
    print 'files saved to disk'
    # random_recos = random_recommendations(
    #     random_user_list, relevant_urlid, no_of_recommendations)
    # print 'random recos generated'
    # #random_recos = spark.createDataFrame(random_recos)
    # random_recos = detach_keys(random_recos, article_id)
    # random_recos = random_recos.drop_duplicates(subset=[user_id,rank_col])
    # random_recos.to_csv(temp_path_for_random_recos,
    #                     sep=sepr_recos, index=False, header=False, append_mode=False)
    # print 'random recos saved'
    # non specialty + speciality users missed in speciality reco users
    # bxu updates - commented
    # non_specialty_user_list = load_list_from_pickle(path_non_specialty_user_list)

    # bxu updates - commented
    # user_missed_in_specialty = find_specialty_missed_users(spark,path_speciality_reco_user_list,temp_path_for_specialty_reco)
    # bxu updates - added
    # results: only missed 11 users

    # bxu updates - commented
    # non_specialty_user_list = find_specialty_missed_users(spark,path_speciality_reco_user_list_all,temp_non_rc_recos_path_s)

    # bxu updates - commented
    # non_specialty_user_list.extend(user_missed_in_specialty)

    # bxu updates
    # commented by bxu
    # if len(non_specialty_user_list)>0:
    #    non_specialty_recos = generate_non_specialty_recos(non_specialty_user_list)
    #    #bxu updates
    #    #added by bxu
    #    #non_specialty_recos = generate_non_specialty_recos2(spark,dh_activity, dh_activity2, dh_activity3, dh_demo, relevant_urlid, no_of_recommendations,\
    #    #article_id, user_id,user_list_for_specialty_based_recos,rank_col,specialty_based)
    #    non_specialty_recos = detach_keys(non_specialty_recos, article_id)
    #    non_specialty_recos.to_csv(temp_path_non_rc_recos_path_s,sep=sepr_recos, index=False, header=False, append_mode=False)

    # combine files
    # bxu updates - commented
    # combine_pyspark_files(temp_path_for_specialty_reco,
    #                      path_recos_for_specialty_based_users)

    # bxu updates - added - then not need anymore
    combine_pyspark_files(temp_non_rc_recos_path_s,
                          path_recos_for_specialty_based_users_s)
    # bxu updates - added - then not need anymore
    combine_pyspark_files(temp_non_rc_recos_path_s,
                          path_recos_for_specialty_based_users_s_bak)
    print 'files combined'


if __name__ == '__main__':
    main()
