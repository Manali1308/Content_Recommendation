#!/home/ubuntu/anaconda/bin/ipython
'''This script will map the users to the reco type. In other words, it will create list of users for different types of reco type'''
# imports
import pandas as pd
import numpy as np
import random
from pyspark.sql.window import Window
# from graph_score_matrix_for_deployement import load_files_and_clean
from config import *
from utils import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# from non_rc_recommendation_generation_engine import *
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


def generate_reco_type_user_list(user_list, dh_activity, pct_random=10):
    """Wrapper function to generate list for
    all the different types(reco types associated with user in recommendation) of users.

    Keyword arguments:
    user_list -- list of all users.
    dh_activity -- pyspark dataframe for activity.
    pct_random -- percentage of graph users(users with more than 5 activities) to be taken as random.

    Output:
    graph_reco_user_list -- list of users(reco type 'g') with high activity.
    graph_random_user_list -- list of users(reco type 'r') chosen to send specialty reco.
    content_reco_user_list -- list of users(reco type 'c') with moderate activity.
    speciality_reco_user_list -- list of users(reco type 's') with no activity.
    """

    graph_reco_user_list, content_reco_user_list, speciality_reco_user_list = find_reco_type_for_user(
        user_list, dh_activity)
    # graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list = reco_type_list_generator(
    #     graph_reco_user_list, content_reco_user_list, speciality_reco_user_list, pct_random=10)
    # graph_reco_user_list, graph_random_user_list, content_reco_user_list, speciality_reco_user_list = reco_type_list_generator(
    #      graph_reco_user_list, content_reco_user_list, speciality_reco_user_list, pct_random=10)
    graph_reco_user_list, graph_random_user_list = segregate_random_reco_user(graph_reco_user_list,
                                                                              pct_random=pct_random)
    content_reco_user_list = remove_users_list(content_reco_user_list, graph_random_user_list)
    speciality_reco_user_list = remove_users_list(speciality_reco_user_list, graph_random_user_list)
    # return graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list
    return graph_reco_user_list, graph_random_user_list, content_reco_user_list, speciality_reco_user_list


def find_reco_type_for_user(user_list, dh_activity):
    """Function will find the reco type for each user based on the activity.
    
    Keyword arguments:
    user_list -- list of all users with specialty assigned to them.
    dh_activity -- pyspark dataframe for activity.

    Output:
    graph_reco_user_list -- list of users with high activity.
    content_reco_user_list -- list of users with moderate activity.
    speciality_reco_user_list -- list of users with no activity.
    """

    # dh_activity = dh_activity.filter(dh_activity.isin(user_list))
    dh_activity = filter_by_join(
        spark, dh_activity, user_list, user_id, common_keys=True)

    df_a = dh_activity.groupby(user_id).agg({user_id: 'count'})
    df_a = df_a.withColumnRenamed('count(' + user_id + ')', 'count_activity')

    graph_reco_user_list = make_unique_col_list(
        df_a.filter(df_a.count_activity >= 5), user_id)
    content_reco_user_list = make_unique_col_list(df_a.filter((df_a.count_activity <= 4) & (
            df_a.count_activity >= 1)), user_id)
    speciality_reco_user_list = list(
        set(user_list) - set(graph_reco_user_list) - set(content_reco_user_list))

    return graph_reco_user_list, content_reco_user_list, speciality_reco_user_list


def find_non_specialty_user(all_user_list):
    '''This function will find the users with no specialty assigned to them.

    Keyword arguments:
    all_user_list -- list of users with specialty assigned.

    Output:
    userlist -- list of users with no specialty.
    '''

    # demo = pd.read_csv(path_demo,sep=sepr)
    demo = load_file_and_select_columns(path_demo, sepr)
    df_miss = demo[~demo.MasterUserID.isin(all_user_list)]
    userlist = df_miss[user_id].unique().tolist()
    return userlist


def segregate_random_reco_user(graph_reco_user_list, pct_random=10):
    '''This function will segregate random and graph reco user list from all graph user list.

    Keyword arguments:
    graph_reco_user_list -- list of all users with high activity(>5).
    pct_random -- percentage of graph users to be chosen as random.

    Output:
    graph_reco_user_list -- list of users who will have 'g' reco type.
    graph_random_user_list -- list of users who will have 'r' reco type.
    '''

    graph_random_user_list = check_and_select_random_user(
        graph_reco_user_list, path_graph_random_user_list, pct_random)
    graph_reco_user_list = remove_users_list(
        graph_reco_user_list, graph_random_user_list)
    return graph_reco_user_list, graph_random_user_list


def check_and_select_random_user(graph_reco_user_list, path_graph_random_user_list, pct_random):
    '''Function to create random user list after every random_cohort_span days.
    It will also return list of random users.

    Keyword arguments:
    graph_reco_user_list -- list of all high activity users.
    path_graph_random_user_list -- path where list of
                                 random users(who will have reco type 'r') will be saved.
    pct_random --  percentage of graph users to be chosen as random.

    Output -- list of users to be sent reco with reco type 'r'.
    '''

    today_date = find_today_date()
    try:
        random_dates = load_list_from_pickle(path_random_user_dates_list)
    except Exception as e:
        # if no pickle existed create random user list and save it
        print e
        random_dates = [today_date]
        graph_random_user_list = select_random_list(graph_reco_user_list, pct_random)
        save_list_as_pickle(random_dates, path_random_user_dates_list)
        save_cohort(path_graph_random_user_folder, graph_random_user_list, today_date)
        return graph_random_user_list
    # find max date
    max_date = np.max(random_dates)
    # find next day of cohort creation
    next_random_date = increase_date(max_date, random_cohort_span)
    if next_random_date <= today_date:
        graph_random_user_list = select_random_list(graph_reco_user_list, pct_random)
        random_dates.append(today_date)
        save_list_as_pickle(random_dates, path_random_user_dates_list)
        save_cohort(path_graph_random_user_folder, graph_random_user_list, today_date)
    else:
        graph_random_user_list = load_list_from_pickle(path_graph_random_user_list)
    return graph_random_user_list


def save_cohort(folder_path, user_list, date):
    '''This function will save the list of users who are selected to be send random recommendation.

    Keyword arguments:
    folder_path -- path of the folder where all cohurts are present.
    user_list -- list of random users.
    date -- date to be appended to file name.
    '''

    path = folder_path + 'graph_random_user_list_' + str(date) + '.p'
    try:
        save_list_as_pickle(user_list, path)
    except Exception as e:
        print e
        os.system('mkdir ' + folder_path)
        save_list_as_pickle(user_list, path)
    print 'random user list created and saved'


# def reco_type_list_generator(graph_reco_user_list, content_reco_user_list, speciality_reco_user_list, pct_random=10):
#     """Function will generate user list for each reco type"""
#     graph_random_user_list = select_random_list(
#         graph_reco_user_list, pct_random)
#     graph_reco_user_list = remove_users_list(
#         graph_reco_user_list, graph_random_user_list)
#     content_random_user_list = select_random_list(
#         content_reco_user_list, pct_random)
#     content_reco_user_list = remove_users_list(
#         content_reco_user_list, content_random_user_list)
#     specialty_random_user_list = select_random_list(
#         speciality_reco_user_list, pct_random)
#     speciality_reco_user_list = remove_users_list(
#         speciality_reco_user_list, specialty_random_user_list)
#     return graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list


def main():
    dh_activity, relevant_urlid, non_rc_approved_articles = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)

    dh_demo = load_demo(spark, path_demo, sepr, cols_demo)
    # generate reco user lists
    all_user_list = make_unique_col_list(dh_demo, user_id)
    graph_reco_user_list, graph_random_user_list, content_reco_user_list, speciality_reco_user_list = generate_reco_type_user_list(
        all_user_list, dh_activity, pct_random=pct_random)
    # non specialiaty inactive user list
    non_specialty_user_list = find_non_specialty_user(all_user_list)
    # save lists as pickle
    save_list_as_pickle(graph_reco_user_list, path_graph_reco_user_list)
    save_list_as_pickle(graph_random_user_list, path_graph_random_user_list)
    save_list_as_pickle(content_reco_user_list, path_content_reco_user_list)
    # save_list_as_pickle(content_random_user_list,
    #                     path_content_random_user_list)
    save_list_as_pickle(speciality_reco_user_list,
                        path_speciality_reco_user_list)
    # save_list_as_pickle(specialty_random_user_list,
    #                     path_specialty_random_user_list)
    save_list_as_pickle(non_specialty_user_list, path_non_specialty_user_list)


if __name__ == '__main__':
    main()
