#!/home/ubuntu/anaconda/bin/ipython
"""This script will be used for recommnedation generation.(RC and non RC)"""
"""This script will take graph affinity scores, content similarity scores and acitvity file to generate custom centre and non custome centre 
recommendations for every user."""

##########################################################################
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


def recent_n_articles(activity_file, no_of_recent_articles):
    """Function will give recent n articles read by a user.

    Keyword arguments:
    activity_file -- pyspark dataframe for activity.
    no_of_recent_articles -- number of recent articles seen by users.

    Output -- pyspark activity dataframe containing only recent activities.
    """

    global user_id, article_id, timestamp
    activity_file = activity_file.withColumn("rank", dense_rank().over(
        Window.partitionBy(user_id).orderBy(desc(timestamp))))
    activity_file = activity_file.drop_duplicates([user_id, 'rank'])
    activity_file = activity_file.filter(
        activity_file.rank <= no_of_recent_articles)
    activity_file = activity_file.drop(timestamp, 'rank')
    return activity_file


##########################################################################
# rc recommendation block
def generate_rc_recos(spark, dh_activity, reco_user_list, score_file, df_approved_links, df_rc_targets, user_id,
                      article_id, id_x, id_y, advertiser_id, score_column, rank_col, reco_type):
    """Function will generate resource center recommendations.
    Here apporved articles for target users may be different. But, users having save advertisers will have
    same approved articles for recommendation.

    Keyword arguments:
    spark -- spark object.
    dh_activity -- pyspark dataframe for activity.
    reco_user_list -- list of target users.
    score_file -- pyspark dataframe for score matrix.
    df_approved_links -- pyspark dataframe for rc approved articles and advertiser id mapping.
    df_rc_targets -- pyspark dataframe for rc target users and advertiser id mapping.
    user_id -- name of the column containing user id(string).
    article_id -- name of the column containing article id(string).
    id_x -- name of the column in score matrix representing articles seen by user(string).
    id_y -- name of the column in score matrix representing articles to be recommended to user(string).
    advertiser_id -- name of the column containing advertiser id(string).
    score_column -- name of the column containing score between pair of articles(string).
    rank_col -- name of the column which will contain rank for the recommendation(string).
    reco_type -- reco type of the recommendation(string).

    Output -- rc recommendation
    """

    # filter activity for users passed in argument
    dh_activity_1 = filter_by_join(spark, dh_activity, reco_user_list, user_id)
    rc_approved_articles_list = make_unique_col_list(
        df_approved_links, article_id)
    # filter score files for rc articles only.
    score_file_1 = filter_by_join(
        spark, score_file, rc_approved_articles_list, id_y)
    # find recent activity of the target users
    dh_activity_1 = recent_n_articles(dh_activity_1, no_of_recent_articles=5)
    # get similar articles with their scores for the target users(joins)
    dh_activity_1 = dh_activity_1.join(df_rc_targets, on=user_id, how='inner')
    score_file_1 = score_file_1.withColumnRenamed(
        id_y, article_id).join(df_approved_links, on=article_id, how='inner')

    dh_activity_1 = dh_activity_1.withColumnRenamed(article_id, id_x).join(
        score_file_1, on=[id_x, advertiser_id], how='inner')
    recommendation_file = dh_activity_1.select(
        [user_id, article_id, score_column])
    # rank the recommended tbids as per their score.
    recommendation_file = groupby_rank(
        recommendation_file, user_id, score_column, rank_col, False)
    # filter the top 50 recommended articles.
    recommendation_file = recommendation_file.filter(
        recommendation_file[rank_col] <= 50)
    # put a column for reco type
    recommendation_file = recommendation_file.withColumn(
        'reco_flag', lit(reco_type))

    return recommendation_file.select([user_id, article_id, rank_col, "reco_flag"])


def generate_rc_specialty_recos(spark, dh_activity, df_rc_targets, user_list, df_approved_links, user_id, article_id,
                                rank_col, advertiser_id, reco_flag):
    """Function will generate rc specialty recos.

    Keyword arguments:
    spark -- spark object.
    dh_activity -- pyspark dataframe for activity.
    df_rc_targets -- pyspark dataframe for rc target users and advertiser id mapping.
    user_list -- list of target users.
    df_approved_links --  pyspark dataframe for rc approved articles and advertiser id mapping.
    user_id -- name of the column containing user id(string).
    article_id -- name of the column containing article id(string).
    rank_col -- name of the column which will contain rank for the recommendation(string).
    advertiser_id -- name of the column containing advertiser id(string).

    Output -- rc specialty recommendation
    """

    # find the number of clicks on each urlid in approved links
    approved_links_list = make_unique_col_list(df_approved_links, article_id)
    dh_activity_1 = filter_by_join(
        spark, dh_activity, approved_links_list, article_id)
    df_rc_targets_1 = filter_by_join(spark, df_rc_targets, user_list, user_id)
    # count number of clicks per article.
    dh_activity_1 = dh_activity_1.groupby(
        article_id).agg(countDistinct(user_id))
    dh_activity_1 = dh_activity_1.withColumnRenamed(
        "count(DISTINCT " + user_id + ")", "users_clicked")
    dh_activity_1 = dh_activity_1.join(
        df_approved_links, on=article_id, how='inner')
    dh_activity_1 = dh_activity_1.join(
        df_rc_targets_1, on=advertiser_id, how='inner')
    # rank articles based on clicks
    dh_activity_1 = groupby_rank(
        dh_activity_1, user_id, "users_clicked", rank_col, False)
    # filter recommendations by rank
    dh_activity_1 = dh_activity_1.filter(dh_activity_1[rank_col] <= 50)
    dh_activity_1 = dh_activity_1.drop("users_clicked")
    # put reco flag
    dh_activity_1 = dh_activity_1.withColumn("reco_flag", lit(reco_flag))
    return dh_activity_1.select([user_id, article_id, rank_col, "reco_flag"])


def generate_rc_random_recos(spark, article_id, df_approved_links, user_list, df_rc_targets, user_id, rank_col,
                             reco_flag):
    """Function will generate random recos for rc target users.

    Keyword arguments:
    spark -- spark object.
    article_id -- name of the column containing article id(string).
    df_approved_links --  pyspark dataframe for rc approved articles and advertiser id mapping.
    user_list -- list of target users.
    df_rc_targets -- pyspark dataframe for rc target users and advertiser id mapping.
    user_id -- name of the column containing user id(string).
    rank_col -- name of the column which will contain rank for the recommendation(string).

    Output -- rc recos for users selected to send random reco
    """

    df_rc_targets_1 = filter_by_join(spark, df_rc_targets, user_list, user_id)
    df_rc_targets_1 = df_rc_targets_1.join(
        df_approved_links, on=advertiser_id, how='inner')
    # add a column with random values
    df_rc_targets_1 = df_rc_targets_1.withColumn("temp", rand())
    df_rc_targets_1 = groupby_rank(
        df_rc_targets_1, user_id, "temp", rank_col, False)
    df_rc_targets_1 = df_rc_targets_1.filter(df_rc_targets_1[rank_col] <= 50)
    df_rc_targets_1 = df_rc_targets_1.drop("temp")
    df_rc_targets_1 = df_rc_targets_1.withColumn("reco_flag", lit(reco_flag))
    return df_rc_targets_1.select([user_id, article_id, rank_col, "reco_flag"])


def generate_rc_missed_recos(spark, article_id, df_approved_links, user_list, df_rc_targets, user_id, rank_col,
                             reco_flag):
    """Function will generate random recos for missed out rc users and give them reco type s.

    Keyword arguments:
    spark -- spark object.
    article_id -- name of the column containing article id(string).
    df_approved_links --  pyspark dataframe for rc approved articles and advertiser id mapping.
    user_list -- list of target users.
    df_rc_targets -- pyspark dataframe for rc target users and advertiser id mapping.
    user_id -- name of the column containing user id(string).
    rank_col -- name of the column which will contain rank for the recommendation(string).

    Output -- rc reco for missed out users
    """

    df_rc_targets_1 = filter_by_join(spark, df_rc_targets, user_list, user_id)
    df_rc_targets_1 = df_rc_targets_1.join(
        df_approved_links, on=advertiser_id, how='inner')

    df_rc_targets_1 = df_rc_targets_1.withColumn("temp", rand())
    df_rc_targets_1 = groupby_rank(
        df_rc_targets_1, user_id, "temp", rank_col, False)
    df_rc_targets_1 = df_rc_targets_1.filter(df_rc_targets_1[rank_col] <= 50)
    df_rc_targets_1 = df_rc_targets_1.drop("temp")
    df_rc_targets_1 = df_rc_targets_1.withColumn("reco_flag", lit(reco_flag))
    return df_rc_targets_1.select([user_id, article_id, rank_col, "reco_flag"])


##########################################################################
# non rc recommendation block


def generate_non_rc_recos(spark, dh_activity, user_list, non_rc_approved_articles, score_file, user_id, article_id,
                          id_x, id_y, score_column, rank_col, reco_type):
    """Function will generate recos for non rc target users.
    Keyword arguments:
    spark -- spark object.
    dh_activity -- pyspark activity dataframe.
    user_list -- list of target users.
    non_rc_approved_articles -- list of non rc approved articles.
    score_file -- pyspark dataframe for score matrix.
    user_id -- name of the column containing user id(string).
    article_id -- name of the column containing article id(string).
    id_x -- name of the column in score matrix representing articles seen by user(string).
    id_y -- name of the column in score matrix representing articles to be recommended to user(string).
    score_column -- list of name of the column containing score between pair of articles(string).
    rank_col -- name of the column which will contain rank for the recommendation(string).
    reco_type --reco type of the recommendation(string).

    Output -- non rc reco file
    """

    # filter activity for the users passed in argument.
    dh_activity_1 = filter_by_join(spark, dh_activity, user_list, user_id)
    # filter score file so that only non rc articles can be recommended.
    score_file_1 = filter_by_join(
        spark, score_file, non_rc_approved_articles, id_y)

    # subset after groupby rank(if reco_type is c then we use both the filters)
    filter_1 = score_column[0]
    filter_2 = score_column[1]

    # define various score columns
    score_column_original = filter_1
    score_column_additional_1 = filter_2
    score_file_1 = subset_by_score_rank(score_file_1, id_x, filter_1, 'rank', non_rc_max_reco * 2)
    score_file_1 = subset_by_score_rank(score_file_1, id_x, filter_2, 'rank', non_rc_max_reco)
    score_file_1 = score_file_1.select([id_x, id_y, filter_2])
    # score_file_1 = score_file_1.withColumnRenamed(filter_2,filter_1)
    # take recently clicked article per user and find articles similar(based on score) to those
    dh_activity_1 = recent_n_articles(dh_activity_1, no_of_recent_articles=5)
    dh_activity_1 = dh_activity_1.join(score_file_1.withColumnRenamed(
        id_x, article_id), on=article_id, how='inner')
    dh_activity_1 = dh_activity_1.drop(article_id)
    dh_activity_1 = dh_activity_1.withColumnRenamed(id_y, article_id)
    # rank articles
    dh_activity_1 = groupby_rank(
        dh_activity_1, user_id, filter_2, rank_col, False)
    dh_activity_1 = dh_activity_1.drop(filter_2)
    # add reco flag
    dh_activity_1 = dh_activity_1.withColumn("reco_flag", lit(reco_type))
    # filter top articles based on rank
    dh_activity_1 = dh_activity_1.filter(dh_activity_1[rank_col] <= non_rc_max_reco)
    return dh_activity_1.select([user_id, article_id, rank_col, "reco_flag"])


# specialty recommendation

def most_popular_relevant_articles_per_specialty(dh_activity, dh_demo, relevant_urlid, no_of_recommendations,
                                                 article_id, user_id):
    """Function will find most popular articles per specialty.

    Keyword arguments:
    dh_activity -- pyspark dataframe for activity.
    dh_demo -- pyspark dataframe for user and specialty mapping.
    relevant_urlid -- list of non rc approved articles.
    no_of_recommendations -- number of recommendations to be sent.
    article_id -- name of the column containing article id(string).
    user_id -- name of the column containing user id(string).
    """

    # only considering relevant articles
    dh_activity = dh_activity[dh_activity[article_id].isin(relevant_urlid)]
    # print dh_activity
    # to get specialty table
    specialty_article_table = dh_activity.join(
        dh_demo, on=user_id, how='inner')
    # print specialty_article_table

    specialty_article_table = specialty_article_table.groupby(
        ['mapped_' + user_specialty, article_id]).agg({user_id: 'count'})
    # print specialty_article_table

    specialty_article_table = groupby_rank(specialty_article_table, 'mapped_' + user_specialty,
                                           'count(' + user_id + ')', rank_col, aesc=False)
    # print specialty_article_table

    specialty_article_table = specialty_article_table.select(
        ['mapped_' + user_specialty, article_id, rank_col])

    # print specialty_article_table

    specialty_article_table = specialty_article_table.filter(
        specialty_article_table[rank_col] <= no_of_recommendations)
    # print specialty_article_table

    return specialty_article_table


# bxu updates
# added
def most_popular_relevant_articles_per_specialty2(dh_activity, dh_activity2, dh_activity3, dh_demo, relevant_urlid,
                                                  no_of_recommendations, article_id, user_id):
    """Function will find most popular articles per specialty.

    Keyword arguments:
    dh_activity -- pyspark dataframe for activity.
    dh_demo -- pyspark dataframe for user and specialty mapping.
    relevant_urlid -- list of non rc approved articles.
    no_of_recommendations -- number of recommendations to be sent.
    article_id -- name of the column containing article id(string).
    user_id -- name of the column containing user id(string).
    """

    # only considering relevant articles
    dh_activity = dh_activity[dh_activity[article_id].isin(relevant_urlid)]
    # bxu updates
    # added by bxu
    dh_activity2 = dh_activity2[dh_activity2[article_id].isin(relevant_urlid)]
    dh_activity3 = dh_activity3[dh_activity3[article_id].isin(relevant_urlid)]

    # print dh_activity
    # to get specialty table
    specialty_article_table = dh_activity.join(
        dh_demo, on=user_id, how='inner')
    # bxu updates
    # added by bxu
    specialty_article_table2 = dh_activity2.join(
        dh_demo, on=user_id, how='inner')
    specialty_article_table3 = dh_activity3.join(
        dh_demo, on=user_id, how='inner')
    # print specialty_article_table

    specialty_article_table = specialty_article_table.groupby(
        ['mapped_' + user_specialty, article_id]).agg({user_id: 'count'})
    # bxu updates
    # added by bxu
    specialty_article_table2 = specialty_article_table2.groupby(
        ['mapped_' + user_specialty, article_id]).agg({user_id: 'count'})
    specialty_article_table3 = specialty_article_table3.groupby(
        ['mapped_' + user_specialty, article_id]).agg({user_id: 'count'})
    # print specialty_article_table

    # bxu updates
    # added by bxu
    specialty_article_table = specialty_article_table.union(specialty_article_table2.union(specialty_article_table3))
    specialty_article_table = specialty_article_table.withColumnRenamed('count(' + user_id + ')', 'counts')
    specialty_article_table = specialty_article_table.groupby(
        ['mapped_' + user_specialty, article_id]).agg({'counts': 'sum'})

    # bxu updates
    # commented by bxu
    # specialty_article_table = groupby_rank(specialty_article_table, 'mapped_' + user_specialty,
    #                                       'count(' + user_id + ')', rank_col, aesc=False)
    # bxu updates
    # added by bxu
    specialty_article_table = groupby_rank(specialty_article_table, 'mapped_' + user_specialty,
                                           'sum(counts)', rank_col, aesc=False)

    # print specialty_article_table

    specialty_article_table = specialty_article_table.select(
        ['mapped_' + user_specialty, article_id, rank_col])

    # print specialty_article_table

    specialty_article_table = specialty_article_table.filter(
        specialty_article_table[rank_col] <= no_of_recommendations)
    # print specialty_article_table

    return specialty_article_table


# bxu updates
# added
def most_popular_relevant_articles_per_specialty3(dh_activity, dh_activity2, dh_activity3, dh_demo, relevant_urlid,
                                                  no_of_recommendations, article_id, user_id):
    """Function will find most popular articles per specialty.

    Keyword arguments:
    dh_activity -- pyspark dataframe for activity.
    dh_demo -- pyspark dataframe for user and specialty mapping.
    relevant_urlid -- list of non rc approved articles.
    no_of_recommendations -- number of recommendations to be sent.
    article_id -- name of the column containing article id(string).
    user_id -- name of the column containing user id(string).
    """

    # only considering relevant articles
    dh_activity = dh_activity[dh_activity[article_id].isin(relevant_urlid)]
    # bxu updates
    # added by bxu
    dh_activity2 = dh_activity2[dh_activity2[article_id].isin(relevant_urlid)]
    dh_activity3 = dh_activity3[dh_activity3[article_id].isin(relevant_urlid)]

    # print dh_activity
    # to get specialty table
    specialty_article_table = dh_activity.join(
        dh_demo, on=user_id, how='inner')
    # bxu updates
    # added by bxu
    specialty_article_table2 = dh_activity2.join(
        dh_demo, on=user_id, how='inner')
    specialty_article_table3 = dh_activity3.join(
        dh_demo, on=user_id, how='inner')
    # print specialty_article_table

    specialty_article_table = specialty_article_table.groupby(
        ['mapped_' + user_specialty, article_id]).agg({user_id: 'count'})
    # bxu updates
    # added by bxu
    specialty_article_table2 = specialty_article_table2.groupby(
        ['mapped_' + user_specialty, article_id]).agg({user_id: 'count'})
    specialty_article_table3 = specialty_article_table3.groupby(
        ['mapped_' + user_specialty, article_id]).agg({user_id: 'count'})
    # print specialty_article_table

    # bxu updates
    # added by bxu
    specialty_article_table = specialty_article_table.union(specialty_article_table2.union(specialty_article_table3))
    specialty_article_table = specialty_article_table.withColumnRenamed('count(' + user_id + ')', 'counts')
    specialty_article_table = specialty_article_table.groupby(
        ['mapped_' + user_specialty, article_id]).agg({'counts': 'sum'})

    # bxu updates
    # commented by bxu
    # specialty_article_table = groupby_rank(specialty_article_table, 'mapped_' + user_specialty,
    #                                       'count(' + user_id + ')', rank_col, aesc=False)
    # bxu updates
    # added by bxu
    specialty_article_table = groupby_rank(specialty_article_table, 'mapped_' + user_specialty,
                                           'sum(counts)', rank_col, aesc=False)

    # print specialty_article_table

    specialty_article_table = specialty_article_table.select(
        ['mapped_' + user_specialty, article_id, rank_col])

    # print specialty_article_table

    specialty_article_table = specialty_article_table.filter(
        specialty_article_table[rank_col] <= no_of_recommendations)
    # print specialty_article_table

    return specialty_article_table


def generate_non_specialty_recos(non_specialty_user_list):
    '''This will generate recos for inactive users not asigned speciality.

    Keyword arguments:
    non_specialty_user_list -- list of target users.
    '''

    # act = pd.read_csv(path_activity,sep='|')
    act = load_file_and_select_columns(path_activity, sepr)
    # generate popularity an article
    df_popular = act.groupby(article_id)[user_id].count().reset_index().sort(user_id, ascending=False).rename(
        columns={user_id: 'count'})
    df_popular = df_popular.head(no_of_recommendations)
    articlelist = df_popular.TBID.unique().tolist()
    userlist = non_specialty_user_list
    m = len(userlist)
    n = len(articlelist)
    ranklist = [i + 1 for i in range(n)]
    userlist = np.repeat(userlist, n)
    articlelist = articlelist * m
    ranklist = ranklist * m
    d = {user_id: userlist, article_id: articlelist, rank_col: ranklist}
    df_reco = pd.DataFrame(data=d)
    df_reco['reco_flag'] = specialty_based
    return df_reco


def find_specialty_missed_users(spark, path_speciality_reco_user_list, temp_path_for_specialty_reco):
    '''This function will find speciality users missed in speciality reco.
    
    Keyword arguments:
    spark -- spark object.
    path_speciality_reco_user_list -- path of list of specialty users(string).
    reco_user_list -- list of target users.
    temp_path_for_specialty_reco -- path of the folder where parts of non rc specialty reco are saved(string).

    Output -- inactive users who were not sent specialty recommendation
    '''

    user_list = load_list_from_pickle(path_speciality_reco_user_list)
    # recos_df = spark.read.csv(
    #     temp_path_for_specialty_reco, sep=sepr_recos, schema=reco_schema)
    recos_df = load_file_and_select_columns(temp_path_for_specialty_reco, sepr_recos, spark=spark, schema=reco_schema)
    reco_user = make_unique_col_list(recos_df, user_id)
    print len(set(user_list) - set(reco_user))
    return list(set(user_list) - set(reco_user))


def find_graph_content_missed_users(spark, path_graph_reco_user_list, path_content_reco_user_list,
                                    temp_non_rc_recos_path):
    '''This function will find graph users missed in graph recos + content users missed in content recos.

    Keyword arguments:
    spark -- spark object.
    path_graph_reco_user_list -- path of list of graph users(string).
    path_content_reco_user_list -- path of list of content users(string).
    temp_non_rc_recos_path -- path of the folder where parts of non rc reco are saved(string).

    Output -- list of users missed from graph and content recommendation.
    '''

    user_list = load_list_from_pickle(path_graph_reco_user_list)
    content_user = load_list_from_pickle(path_content_reco_user_list)
    user_list.extend(content_user)
    # recos_df = spark.read.csv(
    #     temp_non_rc_recos_path, sep=sepr_recos, schema=reco_schema)
    recos_df = load_file_and_select_columns(temp_non_rc_recos_path, sepr_recos, spark=spark, schema=reco_schema)
    reco_user = make_unique_col_list(recos_df, user_id)
    print len(set(user_list) - set(reco_user))
    return list(set(user_list) - set(reco_user))


def non_rc_specialty_recommendations(spark, dh_activity, dh_demo, relevant_urlid, no_of_recommendations, \
                                     article_id, user_id, user_list_for_specialty_based_recos, rank_col, reco_type):
    '''This function will find non rc specialty based recommendations.

    Keyword arguments:
    spark -- spark object.
    dh_activity -- pyspark dataframe for activity.
    dh_demo -- pyspark dataframe for user and specialty mapping.
    relevant_urlid --  list of non rc approved articles.
    no_of_recommendations -- number of recommendations to be sent.
    article_id -- name of the column containing article id(string).
    user_id -- name of the column containing user id(string).
    user_list_for_specialty_based_recos -- list of target users.
    rank_col -- name of the column which will contain rank for the recommendation(string).
    reco_type -- reco type of the recommendation(string).

    Output -- non rc specialty based recommendation.
    '''

    specialty_article_table = most_popular_relevant_articles_per_specialty(
        dh_activity, dh_demo, relevant_urlid, no_of_recommendations, article_id, user_id)
    print 'most popular articles evaluated'
    dh_demo = filter_by_join(
        spark, dh_demo, user_list_for_specialty_based_recos, user_id)
    print 'demo files filtered for users'
    dh_demo = dh_demo.join(specialty_article_table, on='mapped_' +
                                                       user_specialty, how='inner')
    print 'recommendations generated'

    del specialty_article_table
    dh_demo = dh_demo.select([user_id, article_id, rank_col])
    print 'colums selected'
    dh_demo = dh_demo.withColumn('reco_flag', lit(reco_type))
    print 'type column added'
    # drop duplicates
    dh_demo = dh_demo.drop_duplicates(subset=[user_id, rank_col])
    return dh_demo


# bxu updates
# added
def non_rc_specialty_recommendations2(spark, dh_activity, dh_activity2, dh_activity3, dh_demo, relevant_urlid,
                                      no_of_recommendations, \
                                      article_id, user_id, user_list_for_specialty_based_recos, rank_col, reco_type):
    '''This function will find non rc specialty based recommendations.

    Keyword arguments:
    spark -- spark object.
    dh_activity -- pyspark dataframe for activity.
    dh_demo -- pyspark dataframe for user and specialty mapping.
    relevant_urlid --  list of non rc approved articles.
    no_of_recommendations -- number of recommendations to be sent.
    article_id -- name of the column containing article id(string).
    user_id -- name of the column containing user id(string).
    user_list_for_specialty_based_recos -- list of target users.
    rank_col -- name of the column which will contain rank for the recommendation(string).
    reco_type -- reco type of the recommendation(string).

    Output -- non rc specialty based recommendation.
    '''
    # bxu updates
    # commented
    # specialty_article_table = most_popular_relevant_articles_per_specialty(
    #    dh_activity, dh_demo, relevant_urlid, no_of_recommendations, article_id, user_id)
    # bxu updates
    # added
    specialty_article_table = most_popular_relevant_articles_per_specialty2(
        dh_activity, dh_activity2, dh_activity3, dh_demo, relevant_urlid, no_of_recommendations, article_id, user_id)
    print 'most popular articles evaluated'
    dh_demo = filter_by_join(
        spark, dh_demo, user_list_for_specialty_based_recos, user_id)
    print 'demo files filtered for users'
    dh_demo = dh_demo.join(specialty_article_table, on='mapped_' +
                                                       user_specialty, how='inner')
    print 'recommendations generated'

    del specialty_article_table
    dh_demo = dh_demo.select([user_id, article_id, rank_col])
    print 'colums selected'
    dh_demo = dh_demo.withColumn('reco_flag', lit(reco_type))
    print 'type column added'
    # drop duplicates
    dh_demo = dh_demo.drop_duplicates(subset=[user_id, rank_col])
    return dh_demo


# bxu updates
# added
def non_rc_specialty_recommendations3(spark, dh_activity, dh_activity2, dh_activity3, dh_demo, relevant_urlid,
                                      no_of_recommendations, \
                                      article_id, user_id, user_list_for_specialty_based_recos, rank_col, reco_type):
    '''This function will find non rc specialty based recommendations.

    Keyword arguments:
    spark -- spark object.
    dh_activity -- pyspark dataframe for activity.
    dh_demo -- pyspark dataframe for user and specialty mapping.
    relevant_urlid --  list of non rc approved articles.
    no_of_recommendations -- number of recommendations to be sent.
    article_id -- name of the column containing article id(string).
    user_id -- name of the column containing user id(string).
    user_list_for_specialty_based_recos -- list of target users.
    rank_col -- name of the column which will contain rank for the recommendation(string).
    reco_type -- reco type of the recommendation(string).

    Output -- non rc specialty based recommendation.
    '''
    # bxu updates
    # commented
    # specialty_article_table = most_popular_relevant_articles_per_specialty(
    #    dh_activity, dh_demo, relevant_urlid, no_of_recommendations, article_id, user_id)
    # bxu updates
    # added
    specialty_article_table = most_popular_relevant_articles_per_specialty2(
        dh_activity, dh_activity2, dh_activity3, dh_demo, relevant_urlid, no_of_recommendations, article_id, user_id)
    print 'most popular articles evaluated'
    dh_demo = filter_by_join(
        spark, dh_demo, user_list_for_specialty_based_recos, user_id)
    print 'demo files filtered for users'
    dh_demo = dh_demo.join(specialty_article_table, on='mapped_' +
                                                       user_specialty, how='inner')
    print 'recommendations generated'

    del specialty_article_table
    dh_demo = dh_demo.select([user_id, article_id, rank_col])
    print 'colums selected'
    dh_demo = dh_demo.withColumn('reco_flag', lit(reco_type))
    print 'type column added'
    # drop duplicates
    dh_demo = dh_demo.drop_duplicates(subset=[user_id, rank_col])
    return dh_demo

# def generate_non_rc_recos(spark, dh_activity, user_list, non_rc_approved_articles, score_file, user_id, article_id, id_x, id_y, score_column, rank_col, reco_type):
#     """Function will generate recos for non rc target users.

#     Keyword arguments:
#     spark -- spark object.
#     dh_activity -- pyspark activity dataframe.
#     user_list -- list of target users.
#     non_rc_approved_articles -- list of non rc approved articles.
#     score_file -- pyspark dataframe for score matrix.
#     user_id -- name of the column containing user id(string).
#     article_id -- name of the column containing article id(string).
#     id_x -- name of the column in score matrix representing articles seen by user(string).
#     id_y -- name of the column in score matrix representing articles to be recommended to user(string).
#     score_column -- name of the column containing score between pair of articles(string).
#     rank_col -- name of the column which will contain rank for the recommendation(string).
#     reco_type --reco type of the recommendation(string).

#     Output -- non rc reco file
#     """

#     # filter activity for the users passed in argument.
#     dh_activity_1 = filter_by_join(spark, dh_activity, user_list, user_id)
#     # filter score file so that only non rc articles can be recommended.
#     score_file_1 = filter_by_join(
#         spark, score_file, non_rc_approved_articles, id_y)
#     # rank articles based on score and take top non_rc_max_reco articles.

#     score_file_1 = subset_by_score_rank(score_file_1,id_x,score_column,'rank',non_rc_max_reco)
#     score_file_1 = score_file_1.select([id_x,id_y,score_column])
#     print 'subset by score rank'
#     #take recently clicked article per user and find articles similar(based on score) to those
#     dh_activity_1 = recent_n_articles(dh_activity_1, no_of_recent_articles=5)
#     dh_activity_1 = dh_activity_1.join(score_file_1.withColumnRenamed(
#         id_x, article_id), on=article_id, how='inner')
#     dh_activity_1 = dh_activity_1.drop(article_id)
#     dh_activity_1 = dh_activity_1.withColumnRenamed(id_y, article_id)
#     # rank articles
#     dh_activity_1 = groupby_rank(
#         dh_activity_1, user_id, score_column, rank_col, False)
#     dh_activity_1 = dh_activity_1.drop(score_column)    
#     # add reco flag
#     dh_activity_1 = dh_activity_1.withColumn("reco_flag", lit(reco_type))
#     # filter top articles based on rank
#     dh_activity_1 = dh_activity_1.filter(dh_activity_1[rank_col] <= non_rc_max_reco)
#     return dh_activity_1.select([user_id, article_id, rank_col, "reco_flag"])

# def find_missing_users(reco_df, user_id, df_rc_targets, user_list):
#     """Function will find the users missing from reco file."""
#     reco_given_list = make_unique_col_list(reco_df, user_id)

#     reco_eligible_user_list = set(make_unique_col_list(
#         df_rc_targets, user_id)) & set(user_list)

#     reco_missing_user_list = list(
#         reco_eligible_user_list - set(reco_given_list))
#     return reco_missing_user_list

##########################################################################
# main
