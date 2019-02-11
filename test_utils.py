#!/home/ubuntu/anaconda/bin/ipython
"""This script is used to test important functions in recommendation system"""
import pytest
import pickle
import pandas as pd
from config import *
# from test_config import *
from utils import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from reco_validator_and_uploader import *
from graph_score_generator import *
from recommendation_generation_engine import *
from reco_user_list_creator import *

# get spark object
spark = get_spark_session_object()

path_activity_test = '/mnt01/pragalbh/graph_recos/test/generate_reco_type_user_list/dh_activity.txt'
path_graph_reco_user_list = '/mnt01/pragalbh/graph_recos/test/generate_reco_type_user_list/graph_user.p'
path_content_reco_user_list = '/mnt01/pragalbh/graph_recos/test/generate_reco_type_user_list/content_user.p'
path_specialty_reco_user_list = '/mnt01/pragalbh/graph_recos/test/generate_reco_type_user_list/specialty_user.p'
path_all_user = '/mnt01/pragalbh/graph_recos/test/generate_reco_type_user_list/all_user.p'

path_file_load_file_and_select_columns = '/mnt01/pragalbh/graph_recos/test/load_file_and_select_column/file.txt'
sepr_load_file_and_select_columns = ','


def test_find_reco_type_for_user():
    '''This function will test the reco_type_for_user function.'''

    # load the test activity file and demo_file
    dh_activity = load_file_and_select_columns(path_activity_test, sepr=sepr, spark=spark)
    all_user = load_list_from_pickle(path_all_user)
    # call the function
    t_graph_reco_user_list, t_content_reco_user_list, t_specialty_reco_user_list = find_reco_type_for_user(all_user,
                                                                                                           dh_activity)
    # load outputs
    graph_reco_user_list = load_list_from_pickle(path_graph_reco_user_list)
    content_reco_user_list = load_list_from_pickle(path_content_reco_user_list)
    specialty_reco_user_list = load_list_from_pickle(path_specialty_reco_user_list)
    # sort them before checking
    graph_reco_user_list.sort()
    content_reco_user_list.sort()
    specialty_reco_user_list.sort()
    t_graph_reco_user_list.sort()
    t_content_reco_user_list.sort()
    t_specialty_reco_user_list.sort()
    # check
    if (((graph_reco_user_list == t_graph_reco_user_list) & (content_reco_user_list == t_content_reco_user_list) & (
            specialty_reco_user_list == t_specialty_reco_user_list)) == False):
        send_mail("test_find_reco_type_for_user test case failed")
    else:
        print('test_make_unique_col_list test case passed')


def test_pair_wise_occurance():
    '''This function will test pair wise occurance function in graph score generator.'''

    dh_activity = spark.createDataFrame([[1, 101], [1, 102], [2, 102], [2, 101], [3, 101], [3, 102], [3, 103]],
                                        ['MasterUserID', 'TBID'])
    t_graph_df = pair_wise_occurance(dh_activity, user_id, article_id)
    t_graph_df = t_graph_df.toPandas()
    # expected output
    graph_df = pd.DataFrame([[102, 101, 3], [101, 102, 3], [102, 103, 1], [103, 101, 1], [103, 102, 1], [101, 103, 1]], \
                            columns=['urlid_1', 'urlid_2', 'common_count'])
    # check
    if not graph_df.equals(t_graph_df):
        send_mail('pairwise occurance test case failed')
    else:
        print('test_pair_wise_occurance test case passed')


def test_generate_score_for_edge():
    '''This function will test generate score for edge function.'''

    # input
    graph_df = pd.DataFrame([[102, 101, 3], [101, 102, 3], [102, 103, 1], [103, 101, 1], [103, 102, 1], [101, 103, 1]],
                            columns=['urlid_1', 'urlid_2', 'common_count'])
    # call function
    t_degree_1_score = find_score_for_edge(spark.createDataFrame(graph_df), True)
    # convert it to pandas
    t_degree_1_score = t_degree_1_score.toPandas()
    # expected output
    degree_1_score = pd.DataFrame(
        [[103, 101, 0.5], [103, 102, 0.5], [101, 102, 0.75], [101, 103, 0.25], [102, 101, 0.75], [102, 103, 0.25]],
        columns=['urlid_1', 'urlid_2', 'edge_score'])
    # check
    if not degree_1_score.equals(t_degree_1_score):
        send_mail('find score for edge test case failed')
    else:
        print('test_generate_score_for_edge test case passed')


def test_generate_affinity_score():
    '''This function will test generate affinity score function.'''

    # input
    degree_1_score = pd.DataFrame(
        [[103, 101, 0.5], [103, 102, 0.5], [101, 102, 0.50], [101, 103, 0.25], [102, 101, 0.50], [102, 103, 0.25],
         [101, 104, 0.25], [104, 101, 0.5], [102, 104, 0.25], [104, 102, 0.5]],
        columns=['urlid_1', 'urlid_2', 'edge_score'])
    # apply function
    t_final_affinity_score = generate_affinity_score(spark.createDataFrame(degree_1_score), 3)

    t_final_affinity_score = t_final_affinity_score.toPandas()
    # check
    final_affinity_score = pd.DataFrame(
        [[103, 104, 0.225], [102, 101, 0.725], [104, 103, 0.225], [102, 104, 0.3625], [101, 102, 0.725],
         [102, 103, 0.3625], [103, 101, 0.725], [103, 102, 0.725], [104, 102, 0.725], [101, 104, 0.3625],
         [101, 103, 0.3625], [104, 101, 0.725]], \
        columns=[id_x, id_y, 'affinity_score'])
    if not final_affinity_score.equals(t_final_affinity_score):
        send_mail('find affinity score test case failed')
    else:
        print('test_generate_affinity_score test case passed')


def test_generate_non_rc_recos():
    # input variables
    df_act = pd.DataFrame(
        [['2018-06-13 13:59:44', 1, 101], ['2018-06-12 13:59:44', 1, 102], ['2018-06-12 11:59:44', 1, 103],
         ['2018-06-11 13:59:41', 1, 104], \
         ['2018-06-10 13:59:44', 1, 105], ['2018-06-09 13:59:44', 1, 106], ['2018-06-13 13:59:44', 2, 105],
         ['2018-06-12 13:59:44', 2, 106], \
         ['2018-06-11 13:59:44', 2, 107], ['2018-06-10 13:59:44', 2, 108]], columns=[timestamp, user_id, article_id])

    score_file = pd.DataFrame(
        [[105, 1001, 0.785], [105, 1002, 0.03], [105, 1003, 0.05], [105, 1004, 0.80], [105, 1005, 0.001],
         [105, 1006, 0.05], \
         [101, 1007, 0.78], [101, 1008, 0.085], [101, 1009, 0.85], [101, 1016, 0.75], [107, 1010, 0.99], \
         [108, 1011, 0.98], [108, 1012, 0.0005], [108, 1013, 0.0004], [103, 1014, 0.999], [104, 1015, 0.001]],
        columns=[id_x, id_y, 'score_pair'])
    # other variables
    user_list = [1, 2]
    non_rc_approved_articles = range(1001, 1017)
    score_column = 'score_pair'
    rank_col = 'rank'
    reco_type = 'g'
    # convert input to spark dataframes
    df_act = spark.createDataFrame(df_act)
    score_file = spark.createDataFrame(score_file)
    # run the function
    t_reco = generate_non_rc_recos(spark, df_act, user_list, non_rc_approved_articles, score_file, user_id, article_id,
                                   id_x, id_y, score_column, rank_col, reco_type)
    # convert the result to pandas
    t_reco = t_reco.toPandas()
    # expected reco
    reco = pd.DataFrame([[1, 1014, 1, 'g'], [1, 1009, 2, 'g'], [1, 1004, 3, 'g'], [1, 1001, 4, 'g'], [1, 1007, 5, 'g'],
                         [1, 1016, 6, 'g'] \
                            , [1, 1008, 7, 'g'], [1, 1006, 8, 'g'], [1, 1003, 8, 'g'], [1, 1002, 9, 'g'],
                         [1, 1015, 10, 'g'], [1, 1005, 10, 'g'] \
                            , [2, 1010, 1, 'g'], [2, 1011, 2, 'g'], [2, 1004, 3, 'g'], [2, 1001, 4, 'g'],
                         [2, 1006, 5, 'g'], [2, 1003, 5, 'g'] \
                            , [2, 1002, 6, 'g'], [2, 1005, 7, 'g'], [2, 1012, 8, 'g'], [2, 1013, 9, 'g']],
                        columns=[user_id, article_id, rank_col, 'reco_flag'])

    if not reco.equals(t_reco):
        send_mail('non rc recommendation generation test case failed')
    else:
        print('test_generate_non_rc_recos test case passed')


def test_remove_common_rows():
    # input
    df = spark.createDataFrame([[1000000000000, 1000], [2, 2], [3, 2]], ['a', 'b'])

    df_lookback = spark.createDataFrame([[1000000000000, 1000]], ['a', 'b'])
    # call function
    t_output_df = remove_common_rows(spark, df, df_lookback, 'a', 'b', 'a', 'b')

    output_df = spark.createDataFrame([[2, 2], [3, 2]], ['a', 'b'])

    if not t_output_df.toPandas().equals(output_df.toPandas()):
        send_mail("remove_common_rows test case failed")
    else:
        print('test_remove_common_rows test case passed')


def test_load_file_and_select_column():
    # call function
    t_output_1 = load_file_and_select_columns(path_file_load_file_and_select_columns, sepr_load_file_and_select_columns)
    output_1 = pd.DataFrame(
        [[8840873, 745270, 1, 'r'], [8840873, 730070, 2, 'r'], [8840873, 738370, 3, 'r'], [8840873, 739960, 4, 'r'] \
            , [8840873, 724480, 5, 'r']], columns=['MasterUserID', 'TBID', 'rank', 'reco_flag'])
    passed_1 = output_1.equals(t_output_1)
    # call function
    t_output_2 = load_file_and_select_columns(path_file_load_file_and_select_columns, sepr_load_file_and_select_columns,
                                              cols=['MasterUserID', 'reco_flag'])
    output_2 = pd.DataFrame([[8840873, 'r'], [8840873, 'r'], [8840873, 'r'], [8840873, 'r'] \
                                , [8840873, 'r']], columns=['MasterUserID', 'reco_flag'])
    passed_2 = output_2.equals(t_output_2)
    # call function
    t_output_3 = load_file_and_select_columns(path_file_load_file_and_select_columns, sepr_load_file_and_select_columns,
                                              spark=spark)
    output_3 = spark.createDataFrame(
        [[8840873, 745270, 1, 'r'], [8840873, 730070, 2, 'r'], [8840873, 738370, 3, 'r'], [8840873, 739960, 4, 'r'] \
            , [8840873, 724480, 5, 'r']], ['MasterUserID', 'TBID', 'rank', 'reco_flag'])
    output_3 = output_3.toPandas()
    t_output_3 = t_output_3.toPandas()
    passed_3 = output_3.equals(t_output_3)
    # call function
    t_output_4 = load_file_and_select_columns(path_file_load_file_and_select_columns, sepr_load_file_and_select_columns,
                                              cols=['MasterUserID', 'reco_flag'], spark=spark)
    output_4 = spark.createDataFrame([[8840873, 'r'], [8840873, 'r'], [8840873, 'r'], [8840873, 'r'] \
                                         , [8840873, 'r']], ['MasterUserID', 'reco_flag'])
    t_output_4 = t_output_4.toPandas()
    output_4 = output_4.toPandas()
    passed_4 = output_4.equals(t_output_4)
    # call function
    t_output_5 = load_file_and_select_columns(path_file_load_file_and_select_columns, sepr_load_file_and_select_columns,
                                              cols=['MasterUserID', 'reco_flag'], spark=spark, schema=reco_schema)
    output_5 = spark.createDataFrame([[8840873, 'r'], [8840873, 'r'], [8840873, 'r'], [8840873, 'r'] \
                                         , [8840873, 'r']], ['MasterUserID', 'reco_flag'])
    t_output_5 = t_output_5.toPandas()
    output_5 = output_5.toPandas()
    passed_5 = output_5.equals(t_output_5)

    if not (passed_1 & passed_2 & passed_3 & passed_4 & passed_5):
        send_mail('test case failed load_file_and_select_column')
    else:
        print('test_load_file_and_select_column test case passed')


def test_groupby_rank():
    # Input dataframe assignment
    test_df_pandas = pd.DataFrame([[2, 4], [8, 1], [2, 6], [0, 4], [10, 2], [8, 3], [10, 1], [0, 3]],
                                  columns=['a', 'b'])
    test_df_spark_1 = spark.createDataFrame(test_df_pandas)
    test_df_spark_2 = spark.createDataFrame(test_df_pandas)

    # Output dataframe assignment

    check_df_1 = pd.DataFrame(
        [[2, 4, 1.0], [8, 1, 1.0], [2, 6, 2.0], [0, 4, 2.0], [10, 2, 2.0], [8, 3, 2.0], [10, 1, 1.0], [0, 3, 1.0]],
        columns=['a', 'b', 'rank'])
    check_df_2 = pd.DataFrame(
        [[0, 3, 1], [0, 4, 2], [10, 1, 1], [10, 2, 2], [8, 1, 1], [8, 3, 2], [2, 4, 1], [2, 6, 2]],
        columns=['a', 'b', 'rank'])
    check_df_3 = pd.DataFrame(
        [[0, 4, 1], [0, 3, 2], [10, 2, 1], [10, 1, 2], [8, 3, 1], [8, 1, 2], [2, 6, 1], [2, 4, 2]],
        columns=['a', 'b', 'rank'])

    # Calculation of output dataframe with function

    test_df_pandas = groupby_rank(test_df_pandas, 'a', 'b', 'rank')
    test_df_spark_1 = groupby_rank(test_df_spark_1, 'a', 'b', 'rank')
    test_df_spark_2 = groupby_rank(test_df_spark_2, 'a', 'b', 'rank', aesc=False)

    # Converting spark dataframe to pandas

    test_df_spark_1 = test_df_spark_1.toPandas()
    test_df_spark_2 = test_df_spark_2.toPandas()

    # Mail prompt in case function fails

    if (test_df_pandas.equals(check_df_1) == False) | (test_df_spark_1.equals(check_df_2) == False) | (
            test_df_spark_2.equals(check_df_3) == False):
        send_mail('groupby_rank test case failed')
    else:
        print('test_groupby_rank test case passed')


def test_filter_by_join():
    # '''Setting dataframe and list to join'''

    test_df = pd.DataFrame([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=['id_list'])
    test_id_list = [1, 4, 7, 9]

    # '''Converting dataframe to Spark'''

    test_df = spark.createDataFrame(test_df)

    # '''Setting expected output pandas dataframe'''

    check_1 = pd.DataFrame([7, 9, 1, 4], columns=['id_list'])  # common_keys = True
    check_2 = pd.DataFrame([6, 5, 10, 3, 8, 2], columns=['id_list'])  # common_keys = False

    # '''Passing test case to function'''

    test_1 = filter_by_join(spark, test_df, test_id_list, 'id_list', common_keys=True)
    test_2 = filter_by_join(spark, test_df, test_id_list, 'id_list', common_keys=False)

    # '''Converting output spark dataframe to pandas'''

    test_1 = test_1.toPandas()
    test_2 = test_2.toPandas()

    # Mail prompt in case function fails

    if (test_1.equals(check_1) == False) | (test_2.equals(check_2) == False):
        send_mail('filter_by_join test case failed')
    else:
        print('test_filter_by_join test case passed')


def test_make_unique_col_list():
    # input
    test_df_pandas = pd.DataFrame([['A'], ['A'], ['B'], ['C'], ['C'], ['D'], ['D'], ['D']], columns=['Characters'])
    test_df_spark = spark.createDataFrame(test_df_pandas)
    check_pandas = ['A', 'B', 'C', 'D']
    check_spark = [u'C', u'B', u'A', u'D']
    # call function
    test_pandas = make_unique_col_list(test_df_pandas, 'Characters')
    test_spark = make_unique_col_list(test_df_spark, 'Characters')

    if (check_pandas != test_pandas) | (check_spark != test_spark):
        send_mail('make_unique_col_list test case failed')
    else:
        print('test_make_unique_col_list test case passed')


def main():
    '''Main function to test important functions'''
    # test_find_reco_type_for_user
    passed_1 = test_find_reco_type_for_user()
    # test_pair_wise_occurance
    passed_2 = test_pair_wise_occurance()
    # test_generate_score_for_edge
    passed_3 = test_generate_score_for_edge()
    # test_generate_affinity_score
    passed_4 = test_generate_affinity_score()
    # test_generate_non_rc_recos
    passed_5 = test_generate_non_rc_recos()
    # test_remove_common_rows
    passed_6 = test_remove_common_rows()
    # test_load_file_and_select_column
    passed_7 = test_load_file_and_select_column()
    # test_groupby_rank
    passed_8 = test_groupby_rank()
    # test_filter_by_join
    passed_9 = test_filter_by_join()
    # test_make_unique_col_list
    passed_10 = test_make_unique_col_list()


if __name__ == '__main__':
    main()
