#!/home/ubuntu/anaconda/bin/ipython
"""Script will contain all the utility function."""
import random
import pandas as pd
import pickle
from config_s import *
from pyspark.sql.functions import *
import sys
import datetime as dt
import os
from pyspark.sql.window import Window
import numpy as np
from multiprocessing import cpu_count, Pool
from pyspark.sql.types import *
from math import log10, ceil
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def parallelize(data, func):
    """Function will parallelize the apply function"""
    cores = cpu_count()  # Number of CPU cores on your system
    partitions = cores
    data_split = np.array_split(data, partitions)
    pool = Pool(cores)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data


def get_spark_session_object():
    '''This function will create spark session and return spark object'''
    conf = SparkConf()
    conf.setMaster(
        "local[*]").setAppName('App Name').set("spark.executor.memory", "100g").set("spark.driver.memory", "100g").set(
        "spark.result.maxResultSize", "100g")

    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession.builder.master("local[*]").appName("App Name").config(
        "spark.some.config.option", "some-value", conf).getOrCreate()
    print "importing and initialisation finished"
    return spark


def subset_columns(df, column_list):
    """Function will subset pandas dataframe i.e. return only columns of the dataframe which
    are there in column_list.

    Keyword arguments:
    df -- pandas dataframe
    column_list -- list of column names(list of string)

    Output: Pandas dataframe
    """

    return df[column_list]


def save_to_disk(df, path_score, sepr, append_mode=False):
    """Function will save a pyspark dataframe to disk."""
    if append_mode:
        df.toPandas().to_csv(path_score, index=False, sep=sepr, mode='a')
    else:
        df.toPandas().to_csv(path_score, index=False, sep=sepr)


def pyspark_df_to_disk(df, temp_path, sepr, append_mode=False):
    '''Function will save pyspark dataframe to disk'''
    if append_mode:
        df.write.csv(temp_path, sep=sepr, header=False, mode='append')
    else:
        df.write.csv(temp_path, sep=sepr, header=False, mode='overwrite')


def combine_pyspark_files(temp_path, path, col_list=None, sep=','):
    '''Function will combine csv files'''
    if col_list == None:
        cmd_header = 'echo ' + "'" + user_id + sep + article_id + \
                     sep + rank_col + sep + 'reco_flag' + "'" + " " + '> ' + path
    else:
        cmd_header = 'echo ' + sep.join(col_list) + '> ' + path

    cmd_combine = 'cat ' + temp_path + '/' + '*.csv' + ' ' + '>> ' + path
    os.system(cmd_header)
    if os.system(cmd_combine) == 0:
        print 'files combined successfully'


def filter_by_join(spark, df, id_list, column_name, common_keys=True):
    """Function will apply inner join to filter list from a dataframe"""
    # Added by pragalbh on 20th October 2017.
    # dtype_col = df.select(column_name).dtypes[0][1]
    list_df = spark.createDataFrame(
        pd.DataFrame(id_list, columns=[column_name]))
    # type casting to ensure that both columns are of same datatype
    # print dtype_col
    # print list_df
    # list_df = list_df.withColumn(
    #     column_name, list_df[column_name].cast(dtype_col))
    # print list_df

    if common_keys:
        df = df.join(list_df, on=column_name, how='inner')
    else:
        list_df = list_df.withColumn('temp', lit(1))
        df = df.join(list_df, on=column_name, how='left')
        df = df.where(col('temp').isNull())
        df = df.drop('temp')
    return df


def load_file_and_select_columns(path, sepr, cols=None, spark=None, schema=None):
    """Function will load file and select required columns if passed in cols.
        If one wants pandas dataframe, spark variable should be None.
        """
    # if we want to load file as pandas dataframe
    if (spark == None):
        try:
            if cols == None:
                data = pd.read_csv(path, sep=sepr)
                return data
            else:
                data = pd.read_csv(path, sep=sepr)
                data = data[cols]
                return data
        except Exception as e:
            print 'Error : unable to load file at' + path
            print e
            sys.exit(1)
    # if we want to load file as spark dataframe
    else:
        try:
            # if schema is none or
            if schema == None:
                data = spark.read.csv(
                    path, sep=sepr, header=True, inferSchema=True)
            else:
                data = spark.read.csv(
                    path, sep=sepr, header=True, schema=schema)
            if cols == None:
                return data
            else:
                data = data.select(cols)
                return data
        except Exception as e:
            print 'Error: unable to load file at' + path
            print e
            sys.exit(1)


def mapping_fields(df, field):
    """Function will create a map from string to int."""
    # adding an index to the dataframe,selecting the required fields and
    # dropping duplicates so that for every one field there is one mapping
    # index
    try:
        mapping_table = df.withColumn('index', monotonically_increasing_id()).select(
            field, 'index').drop_duplicates([field])
        # merging the dataframe with mapping table on field
        df = df.join(mapping_table, on=field, how='inner')
        # dropping field
        df = df.drop(field)
        # renaming it with mapped field
        df = df.withColumnRenamed('index', 'mapped_' + field)
        print 'creating mapping field completed'
        return df
    except Exception as e:
        print e
        print 'unable to create mapping table'
        return df


def load_activity(spark, path_activity, cols_activity, sepr_activity, article_id):
    """Function will load and clean activity file."""
    dh_activity = load_file_and_select_columns(
        path_activity, sepr_activity, cols_activity, spark)
    try:
        dh_activity = dh_activity.dropna(subset=article_id)
        dh_activity = dh_activity.filter(dh_activity[article_id] != 0)
        dh_activity = dh_activity.drop_duplicates()
        return dh_activity
    except Exception as e:
        print e
        print 'unable to load activity file'
        sys.exit(1)


def find_latest_n_days_activity_data(dh_activity, days_to_consider, date_column):
    """Function will subset lastest n days activity from activity dataframe(both pandas and pyspark)."""
    # find the latest date in activity file
    if (isinstance(dh_activity, pd.DataFrame)):
        try:
            dh_activity[date_column] = pd.to_datetime(dh_activity[date_column])
            max_date = dh_activity[date_column].max()
            last_date_to_consider = max_date - dt.timedelta(days=days_to_consider)
            # subset activity
            dh_activity = dh_activity[dh_activity[date_column] >= last_date_to_consider]
            return dh_activity
        except Exception as e:
            print e
            print 'unable to find latest n days activity data.'
            return dh_activity
    else:
        try:
            latest_available_date = dh_activity.agg(
                {date_column: "max"}).collect()[0][0]
            # finding last date to consider
            last_date_to_consider = latest_available_date - \
                                    dt.timedelta(days=days_to_consider)
            # taking only new activity data
            dh_activity = dh_activity.filter(
                dh_activity[date_column] >= last_date_to_consider)
            return dh_activity
        except Exception as e:
            print e
            print 'unable to find latest n days activity data.'
            return dh_activity


# bxu updates
# added
def find_latest_n_days_activity_data2(dh_activity, days_to_consider, days_to_consider2, days_to_consider3, date_column):
    """Function will subset lastest n days activity from activity dataframe(both pandas and pyspark)."""
    # find the latest date in activity file
    if (isinstance(dh_activity, pd.DataFrame)):
        try:
            dh_activity[date_column] = pd.to_datetime(dh_activity[date_column])
            max_date = dh_activity[date_column].max()
            last_date_to_consider = max_date - dt.timedelta(days=days_to_consider)
            # bxu updates
            # added
            last_date_to_consider2 = max_date - dt.timedelta(days=days_to_consider2)
            last_date_to_consider3 = max_date - dt.timedelta(days=days_to_consider3)
            # subset activity
            dh_activity = dh_activity[dh_activity[date_column] >= last_date_to_consider]
            # bxu updates
            # added
            dh_activity2 = dh_activity[dh_activity[date_column] >= last_date_to_consider2]
            dh_activity3 = dh_activity2[dh_activity2[date_column] >= last_date_to_consider3]
            return dh_activity, dh_activity2, dh_activity3
        except Exception as e:
            print e
            print 'unable to find latest n days activity data.'
            return dh_activity, dh_activity, dh_activity
    else:
        try:
            latest_available_date = dh_activity.agg(
                {date_column: "max"}).collect()[0][0]
            # finding last date to consider
            last_date_to_consider = latest_available_date - dt.timedelta(days=days_to_consider)
            # bxu updates
            # added
            last_date_to_consider2 = latest_available_date - dt.timedelta(days=days_to_consider2)
            last_date_to_consider3 = latest_available_date - dt.timedelta(days=days_to_consider3)
            # taking only new activity data
            dh_activity = dh_activity.filter(dh_activity[date_column] >= last_date_to_consider)
            # bxu updates
            # added
            dh_activity2 = dh_activity.filter(dh_activity[date_column] >= last_date_to_consider2)
            dh_activity3 = dh_activity2.filter(dh_activity2[date_column] >= last_date_to_consider3)
            return dh_activity, dh_activity2, dh_activity3
        except Exception as e:
            print e
            print 'unable to find latest n days activity data.'
            return dh_activity, dh_activity, dh_activity


def load_metadata(spark, path_metadata, cols_metadata, sepr_metadata):
    """Function will load metadata file and will clean it."""
    # clean metadata file, remove urlid==0, convert it to numeric, dropna
    dh_metadata = load_file_and_select_columns(
        path_metadata, sepr_metadata, cols_metadata, spark)
    try:
        # remove where urlid ==0
        dh_metadata = dh_metadata.filter(dh_metadata[article_id] != 0)
        # drop na
        dh_metadata = dh_metadata.dropna()
        dh_metadata = dh_metadata.drop_duplicates()
        dh_metadata = mapping_fields(dh_metadata, article_category)
        print 'cleaning metadata successful'
        return dh_metadata
    except Exception as e:
        print e
        print 'unable to clean metadata file'


def load_demo(spark, path_demo, sepr_demo, cols_demo):
    """Function will load demo file and subset it."""
    # global spark
    # dh_demo = spark.read.csv(path_demo, sep=sepr_demo,
    #                          header=True, inferSchema=True)
    dh_demo = load_file_and_select_columns(path_demo, sepr_demo, spark=spark)
    try:
        dh_demo = dh_demo.select(cols_demo)
        dh_demo = mapping_fields(dh_demo, user_specialty)
        return dh_demo
    except Exception as e:
        print e
        print 'unable to clean demo file'
        sys.exit(1)


# bxu updates - added
def load_demo2(spark, path_demo, sepr_demo, cols_demo):
    """Function will load demo file and subset it."""
    # global spark
    # dh_demo = spark.read.csv(path_demo, sep=sepr_demo,
    #                          header=True, inferSchema=True)
    dh_demo = load_file_and_select_columns(path_demo, sepr_demo, spark=spark)
    try:
        dh_demo = dh_demo.select(cols_demo)

        # bxu updates - added the following lines: replace null by 'missing'
        dh_demo_ns = dh_demo.filter(dh_demo.Specialty.isNull())
        dh_demo_ns = dh_demo_ns.withColumn('Specialty', lit('missing'))
        dh_demo_s = dh_demo.filter(~dh_demo.Specialty.isNull())
        dh_demo = dh_demo_s.union(dh_demo_ns)
        # counts = dh_demo.groupby([user_specialty]).agg({user_id: 'count'})

        dh_demo = mapping_fields(dh_demo, user_specialty)
        return dh_demo
    except Exception as e:
        print e
        print 'unable to clean demo file'
        sys.exit(1)


# bxu updates - added
def load_demo3(spark, path_demo, sepr_demo, cols_demo):
    """Function will load demo file and subset it."""
    # global spark
    # dh_demo = spark.read.csv(path_demo, sep=sepr_demo,
    #                          header=True, inferSchema=True)
    dh_demo = load_file_and_select_columns(path_demo, sepr_demo, spark=spark)
    try:
        dh_demo = dh_demo.select(cols_demo)

        # bxu updates - added the following lines: replace null by 'missing'
        dh_demo_ns = dh_demo.filter(dh_demo.Specialty.isNull())
        dh_demo_ns = dh_demo_ns.withColumn('Specialty', lit('missing'))
        dh_demo_s = dh_demo.filter(~dh_demo.Specialty.isNull())
        # dh_demo=dh_demo_s.union(dh_demo_ns)
        # counts = dh_demo.groupby([user_specialty]).agg({user_id: 'count'})

        # bxu updates - commented
        # dh_demo = mapping_fields(dh_demo, user_specialty)
        # bxu updates - added the following lines: replace null by 'missing'
        dh_demo_ns = mapping_fields(dh_demo_ns, user_specialty)
        dh_demo_s = mapping_fields(dh_demo_s, user_specialty)
        return dh_demo_s, dh_demo_ns
    except Exception as e:
        print e
        print 'unable to clean demo file'
        sys.exit(1)


def find_approved_article_list(dh_metadata, approved_bool, article_id):
    """Function will find approve urlid list."""
    try:
        relevant_urlid = dh_metadata.filter(dh_metadata[approved_bool] == 1).select(
            collect_set(article_id)).collect()[0][0]
        return relevant_urlid
    except Exception as e:
        print e
        print 'unable to find approved articles.'


def find_approved_and_most_popular_article_list(dh_activity, max_num_articles, relevant_urlid, user_id, article_id,
                                                non_rc_approved_articles):
    """Function will return a list with approved and most popular articles."""
    """To be used for filtering."""
    selected_list = []
    rc_approved_articles = list(
        set(relevant_urlid) - set(non_rc_approved_articles))
    # find the number of articles in excess of rc_approved_articles that can be vertex in graph.
    len_non_rc_and_popular_articles = max_num_articles - \
                                      len(rc_approved_articles)
    # if max_num_of_articles is less than total number of rc approved articles
    if len_non_rc_and_popular_articles < 0:
        return find_most_popular_articles(dh_activity, rc_approved_articles, max_num_articles, user_id, article_id,
                                          True)
    else:
        # all all rc approved article to selected list
        selected_list.extend(rc_approved_articles)
        # find the number of articles in excess of rc approved and non rc approved links that can be vertex in graph.
        len_popular_articles = len_non_rc_and_popular_articles - \
                               len(non_rc_approved_articles)
        # if rc_approved and non_rc_approved article fills the max_num_articles limit then find 
        # the most popular articles among the non_rc_approved articles
        if len_popular_articles < 0:
            non_rc_selected = find_most_popular_articles(
                dh_activity, non_rc_approved_articles, len_non_rc_and_popular_articles, user_id, article_id, True)
            selected_list.extend(non_rc_selected)
            return selected_list
        else:
            # else if rc_approved and non_rc_approved articled combined dont fill up max_num_articles, add all non_rc_approved
            # articles to the selected_list and then fill the rest of the space by other
            # (neither in rc nor non_rc approved articles) most popular articles
            selected_list.extend(non_rc_approved_articles)
            popular_articles_list = find_most_popular_articles(
                dh_activity, relevant_urlid, len_popular_articles, user_id, article_id, False)
            selected_list.extend(popular_articles_list)
            return selected_list


def find_most_popular_articles(dh_activity, relevant_urlid, num_most_popular_articles, user_id, article_id,
                               filter_in_bool):
    """Function will find the top "most_popular_articles" number of "relevant_urlid" articles viewed by most number of people."""
    dh_activity1 = dh_activity.select([user_id, article_id])
    if filter_in_bool:
        dh_activity1 = dh_activity1[
            dh_activity[article_id].isin(relevant_urlid)]
    else:
        dh_activity1 = dh_activity1[
            ~dh_activity[article_id].isin(relevant_urlid)]
    dh_activity1 = dh_activity1.drop_duplicates()
    dh_activity1 = dh_activity1.groupby(
        article_id).agg({user_id: "count"})
    dh_activity1 = dh_activity1.withColumnRenamed(
        'count(' + user_id + ')', 'total_views')
    dh_activity1 = dh_activity1.sort(desc("total_views"))
    most_popular_articles_list = dh_activity1.select(
        collect_set(article_id)).collect()[0][0]
    try:
        most_popular_articles_list = most_popular_articles_list[
                                     0:num_most_popular_articles]

    except Exception as e:
        print e
    # print most_popular_articles_list
    return most_popular_articles_list


def save_list_as_pickle(lst, path):
    """Function will save list as pickle."""
    pickle.dump(lst, open(path, "wb"))


def load_list_from_pickle(path):
    """Function will load list as pickle."""
    return pickle.load(open(path, "rb"))


def groupby_rank(df, groupby_on, rank_on, rank_col, aesc=True):
    """Function will groupby using a column and rank using another."""
    # print df
    if (isinstance(df, pd.DataFrame)):
        df[rank_col] = df.groupby(groupby_on)[rank_on].rank(method='dense', ascending=aesc)
    else:
        if aesc:
            df = df.withColumn(rank_col, dense_rank().over(
                Window.partitionBy(groupby_on).orderBy(rank_on)))
        else:
            df = df.withColumn(rank_col, dense_rank().over(
                Window.partitionBy(groupby_on).orderBy(desc(rank_on))))
    # print df
    return df


# @profile
def random_recommendations(user_list, relevant_article_id, no_of_recommendations):
    '''Function will randomly pick n articles for each user'''
    global user_id, article_id
    user_id_list = []
    article_id_list = []
    rank_list = []
    for ids in user_list:
        temp = [ids] * no_of_recommendations
        user_id_list.extend(temp)
        temp_article = random.sample(
            relevant_article_id, no_of_recommendations)
        article_id_list.extend(temp_article)
        random.shuffle(relevant_article_id)
        temp_rank = range(1, no_of_recommendations + 1)
        rank_list.extend(temp_rank)
    random_reco_file = pd.DataFrame(
        {user_id: user_id_list, article_id: article_id_list, rank_col: rank_list})
    random_reco_file['reco_flag'] = random_based
    return random_reco_file


# if __name__ == "__main__":
#     users = random.sample(xrange(100000), 1000)
#     articles = random.sample(xrange(100000), 500)
#     random_recommendations(users, article_id_list, 120)


# def random_recommendations(user_list, relevant_article_id, no_of_recommendations):
#     """Function will randomly pick n articles for each user"""
#     global user_id, article_id, rank_col
#     article_id_list = random.sample(
#         relevant_article_id, no_of_recommendations) * len(user_list)
#     user_id_list = np.repeat(user_list, no_of_recommendations)
#     rank_list = range(1, no_of_recommendations + 1) * len(user_list)
#     print 'lists formed'
#     print 'forming dataframe from lists'
#     random_reco_file = pd.DataFrame(
#         {user_id: user_id_list, article_id: article_id_list, rank_col: rank_list})
#     random_reco_file['reco_flag'] = random_based
#     random_reco_file = random_reco_file[
#         [user_id, article_id, rank_col, 'reco_flag']]
#     # article_id_list = map(float, article_id_list)
#     # user_id_list = map(float, user_id_list)
#     # rank_list = map(float, rank_list)
#     # random_reco_file = spark.createDataFrame(
#     # zip(user_id_list, article_id_list, rank_list), [user_id, article_id,
#     # rank_col])
#     return random_reco_file


def combine_keys(df, primary_key, secondry_key, is_secondry_key_present):
    """Function will make urlid unique using urlid and RC flag and will replace the column."""
    # Inputs: dataframe, primary_key_name, secondry_key_name,
    # new_key_name
    if is_secondry_key_present:
        df[primary_key] = df[primary_key] * 10 + df[secondry_key]
    else:
        df['temp'] = secondry_key
        df[primary_key] = df[primary_key] * 10 + df['temp']
        df = df.drop('temp', axis=1)
    return df


def detach_keys(df, detach_col):
    """Function will convert unique urlid back to urlid and reco flag."""
    # Inputs: dataframe, primary_key_name, secondry_key_name,
    # new_key_name
    if isinstance(df, pd.DataFrame):
        df[detach_col] = (0.1 * df[detach_col]).astype('int')
    else:
        df = df.withColumn(detach_col, df[detach_col] / 10)
        df = df.withColumn(detach_col, df[detach_col].cast(IntegerType()))
    return df


# TODO: put isinstance in this as well
def make_unique_col_list(df, column):
    """Function will return a list of all unique's in the column."""
    if isinstance(df, pd.DataFrame):
        return df[column].unique().tolist()
    else:
        return df.select(collect_set(column)).collect()[0][0]


def file_loader(spark, path_activity, cols_activity_graph, sepr,
                article_id, path_metadata, cols_metadata, path_approved_links,
                cols_approved_links, approved_bool, days_to_consider):
    """Function will load the required files and clean them."""
    # load files and subset
    dh_activity = load_activity(
        spark, path_activity, cols_activity_graph, sepr, article_id)
    dh_activity = find_latest_n_days_activity_data(
        dh_activity, days_to_consider, timestamp)
    dh_metadata = load_metadata(
        spark, path_metadata, cols_metadata, sepr)
    df_approved_links = load_file_and_select_columns(
        path_approved_links, sepr, cols_approved_links, spark)
    # generate non rc approved article list
    non_rc_approved_articles = find_approved_article_list(
        dh_metadata, approved_bool, article_id)
    rc_approved_articles = make_unique_col_list(df_approved_links, article_id)
    relevant_urlid = list(non_rc_approved_articles)
    relevant_urlid.extend(list(rc_approved_articles))
    return dh_activity, relevant_urlid, non_rc_approved_articles


# bxu updates
# added
def file_loader2(spark, path_activity, cols_activity_graph, sepr,
                 article_id, path_metadata, cols_metadata, path_approved_links,
                 cols_approved_links, approved_bool, days_to_consider, days_to_consider2, days_to_consider3):
    """Function will load the required files and clean them."""
    # load files and subset
    dh_activity = load_activity(
        spark, path_activity, cols_activity_graph, sepr, article_id)
    dh_activity, dh_activity2, dh_activity3 = find_latest_n_days_activity_data2(
        dh_activity, days_to_consider, days_to_consider2, days_to_consider3, timestamp)
    dh_metadata = load_metadata(
        spark, path_metadata, cols_metadata, sepr)
    df_approved_links = load_file_and_select_columns(
        path_approved_links, sepr, cols_approved_links, spark)
    # generate non rc approved article list
    non_rc_approved_articles = find_approved_article_list(
        dh_metadata, approved_bool, article_id)
    rc_approved_articles = make_unique_col_list(df_approved_links, article_id)
    relevant_urlid = list(non_rc_approved_articles)
    relevant_urlid.extend(list(rc_approved_articles))
    return dh_activity, dh_activity2, dh_activity3, relevant_urlid, non_rc_approved_articles


def user_list_loader():
    graph_reco_user_list = load_list_from_pickle(path_graph_reco_user_list)
    graph_random_user_list = load_list_from_pickle(path_graph_random_user_list)
    content_reco_user_list = load_list_from_pickle(path_content_reco_user_list)
    # content_random_user_list = load_list_from_pickle(
    #     path_content_random_user_list)
    speciality_reco_user_list = load_list_from_pickle(
        path_speciality_reco_user_list)
    # specialty_random_user_list = load_list_from_pickle(
    #     path_specialty_random_user_list)
    # return graph_reco_user_list, graph_random_user_list, content_reco_user_list, content_random_user_list, speciality_reco_user_list, specialty_random_user_list
    return graph_reco_user_list, graph_random_user_list, content_reco_user_list, speciality_reco_user_list


def remove_users_list(non_random_list, random_list):
    """Function will remove users in second(random_list) list from users in first(non_random_list) list"""
    return list(set(non_random_list) - set(random_list))


def select_random_list(user_list, percentage):
    """Function will select a percentage of users from the list at random."""
    num_users = len(user_list)
    num_random = int(0.01 * percentage * num_users)
    return random.sample(user_list, num_random)


def subset_by_score_rank(df, groupby_on, rank_on, rank_col, top_n_ranks, aesc=False):
    df_1 = groupby_rank(df, groupby_on, rank_on, rank_col, aesc=False)
    # added march 16
    if (isinstance(df, pd.DataFrame)):
        df_1 = df_1[df_1[rank_col] <= top_n_ranks]
        df_1.drop(rank_col, axis=1)
    else:
        df_1 = df_1.filter(df_1[rank_col] <= top_n_ranks)
        df_1 = df_1.drop(rank_col)
    return df_1


def df_to_disk_and_combine(df, temp_path, path, sepr, col_list, append_mode=False):
    pyspark_df_to_disk(df, temp_path, sepr, append_mode)
    combine_pyspark_files(temp_path, path, col_list, sepr)


# bxu update - commented
def find_today_date_0():
    '''This function will find todays date and return it as int'''
    today = dt.datetime.today().strftime('%Y%m%d')
    return int(today)


# bxu update - added
def find_today_date():
    '''This function will find todays date and return it as int'''
    # bxu updates
    # today = dt.datetime.today().strftime('%Y%m%d')
    today = (dt.date.today() - dt.timedelta(delta_today)).strftime('%Y%m%d')
    return int(today)


def increase_date(current_date, increment=1):
    """Take date as input, increase date and return int."""
    date_stamp = dt.datetime.strptime(str(current_date), '%Y%m%d')
    return int((date_stamp + dt.timedelta(increment)).strftime('%Y%m%d'))


def get_integer_tbids(tbids):
    '''This function will get integer values from a list of values(tbids)'''
    int_tbid = []
    for x in tbids:
        if type(x) == int:
            int_tbid.append(x)
    return int_tbid


def get_str_int_tbids(string_tbid):
    '''This function will get those values which are string representation of integer. example '123' from list of values(string_tbids)'''
    str_int_tbids = []
    for x in string_tbid:
        if x.isdigit() or (x.startswith('-') and x[1:].isdigit()):
            str_int_tbids.append(x)
    return str_int_tbids


def convert_string_datetime_to_yyyymmdd(df, col, new_col_name):
    '''This function will convert string datetime format to yyyymmdd(integer) format'''
    df = df.withColumn(new_col_name,
                       from_unixtime(unix_timestamp(col, timestamp_format), "yyyyMMdd").alias(new_col_name).cast(
                           IntegerType()))
    return df


# def make_key_column(df, col1, col2,max_value):
#     """Function will make a key column on the basis of two columns."""
#     scaling_factor = round_up(max_value)
#     df = df.withColumn('common_key', df[col2] * scaling_factor + df[col1])
#     return df
# def find_max(df, col):
#     """Function will find the maximum value of a column in a dataframe."""
#     return df.groupby().max(col).collect()[0][0]


# def round_up(num):
#     """Function will roundup a number to nearest power of 10."""
#     print num
#     return int(10**ceil(log10(num)))

# def myConcat(*cols):
#     '''This function will concatenate the given columns'''
#     return concat(*[coalesce(c, lit("*")) for c in cols])

# def make_key_column(df, col1, col2):
#     """Function will make a key column on the basis of two columns."""

#     df = df.withColumn('common_key', myConcat(col1,col2))
#     return df

def remove_common_rows(spark, parent_df, lookback_df, col_1, col_2):
    '''Function will remove rows from  parent_df dataframe which has common col_1 and col_2 with lookback_df dataframe'''

    if ((lookback_df.count() == 0) | (parent_df.count() == 0)):
        return recos_df
    else:
        parent_df = parent_df.join(lookback_df, [col_1, col_2], "leftanti")
        print 'filtered lookback_df out of parent_df'
        return parent_df
