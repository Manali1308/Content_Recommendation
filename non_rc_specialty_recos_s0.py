#!/home/ubuntu/anaconda/bin/ipython
"""Script will generate recommendations for inactive users plus the users who are missed from graph and content recommendations."""
try:
    # import graph_score_matrix_for_deployement as gsm
    # import libraries
    import pandas as pd
    from utils_s import *
    from config_s import *
    # from graph_score_matrix_for_deployement import load_files_and_clean
    import pandas as pd
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
    user_list_for_specialty_based_recos = load_list_from_pickle(
        path_speciality_reco_user_list)
    # append the graph-content user who missed getting graph-content reco
    try:
        user_missed_in_graph_content = find_graph_content_missed_users(spark, path_graph_reco_user_list,
                                                                       path_content_reco_user_list,
                                                                       temp_non_rc_recos_path)
        user_list_for_specialty_based_recos.extend(user_missed_in_graph_content)
    except Exception as e:
        print e
    # non rc specialty recommendtions
    # bxu updates
    # commented by bxu
    # specialty_reco = non_rc_specialty_recommendations(spark,dh_activity, dh_demo, relevant_urlid, no_of_recommendations,\
    # article_id, user_id,user_list_for_specialty_based_recos,rank_col,specialty_based)

    # bxu updates
    # commented by bxu
    ## detach keys
    # specialty_reco = detach_keys(specialty_reco, article_id)
    # save to disk
    # pyspark_df_to_disk(
    #   specialty_reco, temp_path_for_specialty_reco, sepr_recos, append_mode=False)
    # del specialty_reco
    # print 'files saved to disk'

    # find the users who do not have a specialty or their specialty does not have any activity in past 3 months
    non_specialty_user_list = load_list_from_pickle(path_non_specialty_user_list)
    # bxu updates
    # commented by bxu
    # user_missed_in_specialty = find_specialty_missed_users(spark,path_speciality_reco_user_list,temp_path_for_specialty_reco)
    # non_specialty_user_list.extend(user_missed_in_specialty)

    # bxu updates
    # added by bxu
    user_list_for_specialty_based_recos.extend(non_specialty_user_list)

    # bxu updates
    # added by bxu
    specialty_reco = non_rc_specialty_recommendations2(spark, dh_activity, dh_activity2, dh_activity3, dh_demo,
                                                       relevant_urlid, no_of_recommendations, \
                                                       article_id, user_id, user_list_for_specialty_based_recos,
                                                       rank_col, specialty_based)

    # bxu updates
    # added by bxu
    ## detach keys
    specialty_reco = detach_keys(specialty_reco, article_id)
    # save to disk
    pyspark_df_to_disk(
        specialty_reco, temp_path_for_specialty_reco, sepr_recos, append_mode=False)
    del specialty_reco
    print 'files saved to disk'

    # generate reco for above mentioned users
    # bxu updates
    # commented by bxu
    # non_specialty_recos = generate_non_specialty_recos(non_specialty_user_list,relevant_urlid)

    # bxu updates
    # commented by bxu
    # non_specialty_recos = detach_keys(non_specialty_recos, article_id)
    # non_specialty_recos.to_csv(temp_path_non_specialty_recos ,sep=sepr_recos, index=False, header=False, append_mode=False)
    ##save the users who are sent specialty recommendations
    save_list_as_pickle(user_missed_in_graph_content, path_list_graph_content_missed_users)
    ##combine files
    combine_pyspark_files(temp_path_for_specialty_reco,
                          path_recos_for_specialty_based_users)
    print 'files combined'


if __name__ == '__main__':
    main()
