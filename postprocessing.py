#!/home/ubuntu/anaconda/bin/ipython
"""Script will postprocess the recommendations."""
from utils import *
from config import *
from reco_validator_utils import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pandas as pd
from pyspark.sql.functions import *
from math import log10, ceil
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *
import numpy as np
from pyspark.sql import Row

conf = SparkConf()
conf.setMaster(
    "local[*]").setAppName('App Name').set("spark.executor.memory", "100g").set("spark.driver.memory", "100g")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.master("local[*]").appName("App Name").config(
    "spark.some.config.option", "some-value", conf).getOrCreate()


def removed_already_viewed_articles(spark, recos_df, lookback_df, col_1, col_2):
    '''Function will remove common user id and article id in lookback_df dataframe and recos_df dataframe'''

    recos_df = remove_common_rows(spark, recos_df, lookback_df, col_1, col_2)
    return recos_df


def rerank(recos_df, user_id, rank_col):
    """Function will rerank recommendations."""
    recos_df = groupby_rank(recos_df, user_id, rank_col, rank_col)
    return recos_df


def remove_duplicate_recommendations(df, user_id, article_id):
    """Function will drop duplicates recommendations."""
    df = df.drop_duplicates(subset=[user_id, article_id])
    return df


def take_fewer_recos(df_recos, n_reco, rank_col):
    '''This function will take less than or equal to n_cc_reco per user(assuming the rank_cols are having consequtive numbers per user)'''
    df_recos = df_recos.filter(df_recos[rank_col] <= n_reco)
    df_recos = df_recos.drop_duplicates(subset=[user_id, rank_col])
    return df_recos


def copy_dump_elsewhere(rc_reco_dump_elsewhere, temp_path_rc_reco_dump_file):
    '''This function will copy dump from its path to some temporary path to deal with pyspark overwrite issue'''
    if (os.system('rm -rf ' + rc_reco_dump_elsewhere) == 0):
        if (os.system(
                'test -d ' + rc_reco_dump_elsewhere + ' || mkdir -p ' + rc_reco_dump_elsewhere + '&& cp -Rv ' + temp_path_rc_reco_dump_file + '/*' + ' ' + rc_reco_dump_elsewhere) == 0):
            print 'copying dump done'
        else:
            print 'copying dump failed'
    else:
        print 'removing failed'


def cc_modify_dump_restriction_file_and_save(spark, df_daily_reco, n_cc_restriction, user_id, article_id,
                                             temp_path_rc_reco_dump_file, temp_path_cc_restriction_lookback, rank_col,
                                             sepr_restriction_dump):
    '''Function will modify the dump and restriction file'''
    # create a column num_times in the folder
    df_daily_reco = df_daily_reco.withColumn('num_times', lit(1)).select(user_id, article_id, 'num_times')
    pyspark_df_to_disk(df_daily_reco, temp_path_rc_reco_dump_file, sepr_restriction_dump, append_mode=True)
    # copy the dump files in some other folder
    copy_dump_elsewhere(rc_reco_dump_elsewhere_path, temp_path_rc_reco_dump_file)
    # load from other folder
    # df_reco_dump_file = load_files(spark,rc_reco_dump_elsewhere_path,sepr_restriction_dump,dump_schema)
    df_reco_dump_file = load_file_and_select_columns(
        rc_reco_dump_elsewhere_path, sepr_restriction_dump, spark=spark, schema=dump_schema)
    # find more than n_cc_restriction recommendations
    df_agg = df_reco_dump_file.groupby(
        user_id, article_id).agg({'num_times': "sum"})

    df_more_than_n_times = df_agg.filter(df_agg['sum(' + 'num_times' + ')'] > n_cc_restriction)
    # remove these recos from dump file
    df_agg = removed_already_viewed_articles(spark, df_agg, df_more_than_n_times, user_id, article_id)
    # save df_agg to dump file
    pyspark_df_to_disk(df_agg, temp_path_rc_reco_dump_file, sepr_restriction_dump, append_mode=False)
    # append restriction recos
    pyspark_df_to_disk(df_more_than_n_times.select(user_id, article_id), temp_path_cc_restriction_lookback,
                       sepr_restriction_dump, append_mode=True)
    print 'restriction updated'


def rank_on_row_number(df, groupby_on, row_number_on, row_col):
    '''This function will groupby on groupby_on column and assign rank based on row number'''
    df = df.withColumn(row_col, row_number().over(Window.partitionBy(groupby_on).orderBy(row_number_on)))
    return df


def get_recos_per_sentiment(post_recos, sentiment_value):
    '''Function to generate recommendations using headline sentiment_value for each user'''
    df = post_recos.filter(post_recos[col_sentiment_tag] == sentiment_value)
    df = df.drop(col_sentiment_tag)
    df_1 = groupby_rank(df, user_id, rank_col, 'rank_temp')
    # p_2 = df_1.toDF('MasterUserID', 'TBID', 'rank', 'reco_flag')
    df_1 = rank_on_row_number(df_1, user_id, 'rank_temp', 'row_number')
    df_1 = df_1.filter(df_1['row_number'] <= 3)
    df_1 = df_1.drop_duplicates(subset=[user_id, 'rank_temp'])
    df_1 = df_1.drop('rank_temp')
    df_1 = df_1.drop('row_number')
    return df_1


def generate_sentiment_wise_recommendations(post_recos, sentiment_df):
    '''Function to merge cattopic recommendation file with headline sentiment file and choose top 3 articles for positive
        negative and neutral sentiment'''
    # Merging cattopic recommendation file with article sentiment file
    score_df = post_recos.join(sentiment_df, on=article_id, how='inner')
    score_df = score_df.select([user_id, article_id, rank_col, 'reco_flag', col_sentiment_tag])
    # Function call for generating sentiment wise recommendation for each user
    # Positive tag top 3 recommended articles
    pos_ranked = get_recos_per_sentiment(score_df, 1)
    # Negative tag top 3 recommended articles
    neg_ranked = get_recos_per_sentiment(score_df, -1)
    # Neutral tag top 3 recommended articles
    neut_ranked = get_recos_per_sentiment(score_df, 0)
    # Combining positive, negative and neutral sentiment 
    sentiment_recos = pos_ranked.union(neg_ranked)
    sentiment_recos = sentiment_recos.union(neut_ranked)
    # Rank the combined dataframe based on cattopic score rank for every user
    sentiment_recos = groupby_rank(sentiment_recos, user_id, rank_col, rank_col)
    return sentiment_recos


def find_remaining_recommendation(post_recos, sentiment_recos):
    '''Function counts the number of more articles required to make 20 recommendations '''
    # Counting total number of sentiment recommendations collected for every user in columns 'num_reco_per_user'
    df_count_recos_per_user = sentiment_recos.groupby(user_id).agg({article_id: 'count'})
    df_count_recos_per_user = df_count_recos_per_user.withColumnRenamed('count(' + article_id + ')',
                                                                        'num_reco_per_user')
    # Evaluating remaining number of recos by subtracting generated recos from number of required recos
    df_count_recos_per_user = df_count_recos_per_user.withColumn('remaining_num_of_recos',
                                                                 no_of_recommendations - df_count_recos_per_user[
                                                                     'num_reco_per_user'])
    # Removing common rows in columns article_id and user_id between post_recos and sentiment_recos dataframe
    p_1 = post_recos.toDF(user_id, article_id, rank_col, 'reco_flag')
    post_recos = remove_common_rows(spark, p_1, sentiment_recos, user_id, article_id)
    # Ranking articles based on cattopic score rank from the articles remaining
    post_recos = groupby_rank(post_recos, user_id, rank_col, rank_col)
    p = post_recos.toDF(user_id, article_id, rank_col, 'reco_flag')
    # Join post recos with df_count_recos_per_user 
    post_recos = df_count_recos_per_user.join(p, on=user_id, how='inner')
    return post_recos


def generate_final_recos(post_recos, sentiment_recos):
    '''Function generated the final recommendation file by merging sentimentwise-filtered and remaining recos and removing duplicates'''
    post_recos = post_recos[post_recos[rank_col] <= post_recos['remaining_num_of_recos']]
    # Increasing generated rank by num_reco_per_user to avoid duplication with sentiment_recos ranks
    post_recos = post_recos.withColumn(rank_col, post_recos[rank_col] + post_recos['num_reco_per_user'])
    # Dropping columns remaining_num_of_recos and num_reco_per_user
    post_recos = post_recos.drop('remaining_num_of_recos', 'num_reco_per_user')
    # Merging dataframe post_recos with sentiment_recos
    post_recos = post_recos.union(sentiment_recos)
    # Ranking articles of final recommendation file for every user 
    post_recos = groupby_rank(post_recos, user_id, rank_col, rank_col)
    # rank on row number to overcome the problem of duplicate ranks
    post_recos = rank_on_row_number(post_recos, user_id, rank_col, rank_col)
    return post_recos


def shuffling_reco_wrapper(post_processed_df, path_sentiment_score):
    '''Master Function to generate headline sentiment based recommendations for non rc users '''
    sentiment_df = load_file_and_select_columns(path_sentiment_score, sepr=sepr, spark=spark,
                                                cols=[article_id, col_sentiment_tag])
    post_processed_df_non_specialty = post_processed_df.filter(post_processed_df['reco_flag'] != random_based)
    post_processed_df_specialty = post_processed_df.filter(post_processed_df['reco_flag'] == random_based)
    sentiment_recos = generate_sentiment_wise_recommendations(post_processed_df_non_specialty, sentiment_df)
    post_recos_non_specialty = find_remaining_recommendation(post_processed_df_non_specialty, sentiment_recos)
    post_recos_non_specialty = generate_final_recos(post_recos_non_specialty, sentiment_recos)
    post_recos = post_recos_non_specialty.union(post_processed_df_specialty)
    return post_recos


def cross_join(list1, list2, col1, col2):
    '''This function will do a cartesian join using two lists.'''
    R_1 = Row(col1)
    R_2 = Row(col2)
    df_1 = spark.createDataFrame([R_1(i) for i in list1])
    df_2 = spark.createDataFrame([R_2(i) for i in list2])
    df_cross = df_1.crossJoin(df_2)
    return df_cross


def generate_backfilling_reco_crossjoin(approved_articles, non_rc_recos_df_less_reco_user_list, user_id, article_id,
                                        non_rc_recos_df_less_reco_flag):
    '''This function will generate random reco after cross joining'''
    random_reco = cross_join(non_rc_recos_df_less_reco_user_list, approved_articles, user_id, article_id)
    random_reco = rank_on_row_number(random_reco, user_id, article_id, rank_col)
    # rerank these random reco
    random_reco = random_reco.withColumn(rank_col, random_reco[rank_col] + non_rc_max_reco)
    # add reco flag
    random_reco = random_reco.join(non_rc_recos_df_less_reco_flag, on=user_id, how='left').fillna(random_based,
                                                                                                  subset=['reco_flag'])

    return random_reco.select([user_id, article_id, rank_col, "reco_flag"])


def find_users_missed_after_postprocessing(non_rc_recos_df, graph_reco_user_list, graph_random_user_list, \
                                           content_reco_user_list, path_list_graph_content_missed_users):
    '''This function will find users who have no recommendation after postprocessing'''
    reco_users = make_unique_col_list(non_rc_recos_df, user_id)
    missed_users_sent_specialty_reco = load_list_from_pickle(path_list_graph_content_missed_users)
    to_be_recommended_user = graph_reco_user_list
    to_be_recommended_user.extend(graph_random_user_list)
    to_be_recommended_user.extend(content_reco_user_list)

    return list(set(to_be_recommended_user) - set(reco_users) - set(missed_users_sent_specialty_reco))


def back_filling_reco_for_user_having_less_reco(spark, non_rc_recos_df, non_rc_lookback_df, no_of_recommendations,
                                                path_metadata, approved_bool):
    '''Add random articles for users who receive less number of recommendations'''
    meta_data = load_file_and_select_columns(path_metadata, sepr=sepr)
    approved_articles = meta_data[meta_data[approved_bool] == True][article_id].unique().tolist()
    # find users with less reco
    non_rc_recos_df_less_reco = non_rc_recos_df.groupby(user_id).agg({article_id: 'count'})
    non_rc_recos_df_less_reco_count = non_rc_recos_df_less_reco.filter(
        non_rc_recos_df_less_reco['count(' + article_id + ')'] < no_of_recommendations)
    non_rc_recos_df_less_reco_user_list = make_unique_col_list(non_rc_recos_df_less_reco_count, user_id)
    print non_rc_recos_df_less_reco_user_list

    graph_reco_user_list, graph_random_user_list, content_reco_user_list, _ = user_list_loader()
    users_missed = find_users_missed_after_postprocessing(non_rc_recos_df, graph_reco_user_list, graph_random_user_list,
                                                          content_reco_user_list, path_list_graph_content_missed_users)
    non_rc_recos_df_less_reco_user_list.extend(users_missed)
    print len(non_rc_recos_df_less_reco_user_list)

    if len(non_rc_recos_df_less_reco_user_list) > 0:
        non_rc_recos_df_less_reco_flag = filter_by_join(spark, non_rc_recos_df, non_rc_recos_df_less_reco_user_list,
                                                        user_id) \
            .select([user_id, 'reco_flag']).drop_duplicates()

        # find lookback articles for these users
        non_rc_lookback_df_1 = filter_by_join(spark, non_rc_lookback_df, non_rc_recos_df_less_reco_user_list, user_id)
        non_rc_lookback_df_articles = make_unique_col_list(non_rc_lookback_df_1, article_id)
        # remaining articles
        approved_articles = list(set(approved_articles) - set(non_rc_lookback_df_articles))[:200]
        random_reco = generate_backfilling_reco_crossjoin(approved_articles, non_rc_recos_df_less_reco_user_list,
                                                          user_id, article_id, non_rc_recos_df_less_reco_flag)
        # append random reco
        non_rc_recos_df = non_rc_recos_df.union(random_reco)
        non_rc_recos_df = rerank(non_rc_recos_df, user_id, rank_col)
    return non_rc_recos_df


def main():
    # load rc reco df
    try:
        rc_recos_df = load_file_and_select_columns(rc_reco_path, sepr_recos, spark=spark, schema=reco_schema)
        print 'cc_recos file loaded'
        # rc remove duplicate recommendations
        rc_recos_df = remove_duplicate_recommendations(
            rc_recos_df, user_id, article_id)
        print 'cc duplicate recos dropped'
        # rload rc lookback file
        # rc_lookback_df = load_files(spark, path_rc_lookback_file, sepr)
        rc_lookback_df = load_file_and_select_columns(path_rc_lookback_file, sepr, spark=spark)
        print 'cc lookback_file loaded'
        # remove already viewed articles from reco
        rc_recos_df = removed_already_viewed_articles(
            spark, rc_recos_df, rc_lookback_df, user_id, article_id)
        print 'cc lookback ids removed'
        # load restriction lookback file
        try:
            # df_rc_restriction_lookback = load_files(
            #     spark, temp_path_cc_restriction_lookback, sepr_restriction_dump,restriction_schema)
            df_rc_restriction_lookback = load_file_and_select_columns(temp_path_cc_restriction_lookback,
                                                                      sepr_restriction_dump, spark=spark,
                                                                      schema=restriction_schema)
            print 'restriction file was present and loaded'
        except Exception as e:
            print e
            df_rc_restriction_lookback = spark.createDataFrame([], schema=restriction_schema)
        print 'cc restriction lookback file loaded'
        # remove recos which are already in restriction lookback file
        try:
            rc_recos_df = remove_common_rows(spark, rc_recos_df, df_rc_restriction_lookback, user_id, article_id)
            print 'cc restriction ids removed'
        except:
            print 'cc restriction ids not removed'
        # reco_validator check to see if rc restriction recos are removed or not
        try:
            check_if_rc_restriction_recos_are_removed(spark, rc_recos_df, df_rc_restriction_lookback)
        except:
            send_mail('rc restriction reco remove check failed')
        # do the reranking of recos
        rc_recos_df = rerank(rc_recos_df, user_id, rank_col)
        print 're-ranking completed'
        # take few recos per user
        rc_recos_df = take_fewer_recos(rc_recos_df, n_cc_reco, rank_col)
        print 'fewer cc recos taken per user'
        # modify the dump file
        try:
            cc_modify_dump_restriction_file_and_save(spark, rc_recos_df, n_cc_restriction, user_id, article_id,
                                                     temp_path_rc_reco_dump_file, temp_path_cc_restriction_lookback,
                                                     rank_col, sepr_restriction_dump)
            print 'dump restriction file modification done'
        except Exception as e:
            print e
            print 'dump restriction file modification not happened'
        # detach key
        rc_recos_df = detach_keys(rc_recos_df, article_id)
        # save recos
        pyspark_df_to_disk(rc_recos_df, temp_rc_recos_path,
                           sepr_recos, append_mode=False)
        # combine rc reco csv files
        combine_pyspark_files(temp_rc_recos_path,
                              rc_reco_path)
        print 'rc recos saved'
    except Exception as e:
        print e
        print 'unable to postprocess rc file'
    ############################non rc postprocessing#########################
    try:
        # load non rc reco file as dataframe
        # non_rc_recos_df = load_files(spark, non_rc_reco_path, sepr_recos,reco_schema)
        non_rc_recos_df = load_file_and_select_columns(non_rc_reco_path, sepr_recos, spark=spark, schema=reco_schema)
        print 'non rc recos file loaded'
        # remove duplicate recommendations
        non_rc_recos_df = remove_duplicate_recommendations(
            non_rc_recos_df, user_id, article_id)
        print 'non rc duplicate recos dropped'
        # load non rc lookback file
        # non_rc_lookback_df = load_files(spark, path_non_rc_lookback_file, sepr,restriction_schema)
        non_rc_lookback_df = load_file_and_select_columns(
            path_non_rc_lookback_file, sepr, spark=spark, schema=restriction_schema)
        print 'non rc lookback file loaded'
        # remove already viewed articles from non-rc reco df
        non_rc_recos_df = removed_already_viewed_articles(
            spark, non_rc_recos_df, non_rc_lookback_df, user_id, article_id)
        print 'non rc already viewed files removed'
        # rerank the recos
        non_rc_recos_df = rerank(non_rc_recos_df, user_id, rank_col)
        print 'non rc re-ranking completed'
        # Shuffling recommendations based on article headline sentiment
        try:
            non_rc_recos_df = shuffling_reco_wrapper(non_rc_recos_df, path_sentiment_score)
            print 'shuffling done'
        except Exception as e:
            print e
            print 'shuffling not happened'
        # take only few recos per user
        try:
            non_rc_recos_df = back_filling_reco_for_user_having_less_reco(spark, non_rc_recos_df, \
                                                                          non_rc_lookback_df, no_of_recommendations,
                                                                          path_metadata, approved_bool)
            print 'backfilling done'
        except Exception as e:
            print e
            print 'backfilling not happened'

        non_rc_recos_df = take_fewer_recos(
            non_rc_recos_df, n_non_cc_reco, rank_col)
        print 'fewer non cc recos taken per user'
        # detach key
        non_rc_recos_df = detach_keys(non_rc_recos_df, article_id)
        # save recos
        pyspark_df_to_disk(
            non_rc_recos_df, temp_non_rc_recos_path, sepr_recos, append_mode=False)
        # combine non rc reco csv files
        combine_pyspark_files(temp_non_rc_recos_path,
                              non_rc_reco_path)
    except Exception as e:
        print e
        print 'unable to postprocess non rc file'


if __name__ == '__main__':
    main()
