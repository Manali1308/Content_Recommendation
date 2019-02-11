#!/home/ubuntu/anaconda/bin/ipython
##########################################################################
"""Script will generate graph score_matrix between articles.

Activity data is used to create a bidirectional weighted graph.
3 different scores are calculated from the graph for each pair(directed pair) of articles and the scores are merged:
These scores are affinity score, degree centrality score and eigen vector centrality score.
"""

# import block
from utils import *
from config import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import *
import datetime as dt
from pyspark.sql.functions import *
import networkx as nx
import pandas as pd

# conf = SparkConf()
# conf.setMaster(
#     "local[*]").setAppName('App Name').set("spark.executor.memory", "100g").set("spark.driver.memory", "100g").set("spark.result.maxResultSize", "100g")

# sc = SparkContext.getOrCreate(conf=conf)
# spark = SparkSession.builder.master("local[*]").appName("App Name").config(
#     "spark.some.config.option", "some-value", conf).getOrCreate()
# print "importing and initialisation finished"

spark = get_spark_session_object()


##########################################################################


def pair_wise_occurance(dh_activity, user_id, article_id):
    """Function will generate the pair wise occurance matrix for every urlid pair.
        Suppose article '1' and '2' are clicked by 4 common users, then ('1','2',4) and ('2','1',4) 
        will be entered in the output dataframe.

        Keyword arguements:
        dh_activity -- spark activity dataframe.
        user_id -- name of the column containing user's id in dh_activity.
        article_id -- name of the column containing article's id in dh_activity.
        
        Output:
        spark dataframe containing count of common users who viewed 2 articles taken at a time.
    """
    # selecting only required columns from activity file
    # activity data, i.e. masteruserid,urlid,timestamp is the input and
    # 'urlid_1','urlid_2','common_count' is the output
    dh_activity = dh_activity.select(user_id, article_id)
    # dropping duplicates
    dh_activity = dh_activity.drop_duplicates()
    # cross join on activity file
    dh_activity = dh_activity.select(user_id, \
                                     dh_activity[article_id].alias('urlid_1')). \
        join(dh_activity.select(user_id, dh_activity[article_id].alias('urlid_2')), \
             on=user_id, how='inner')
    # dropping duplicates
    dh_activity = dh_activity.drop_duplicates()
    # filtering self edges
    dh_activity = dh_activity.filter(
        dh_activity.urlid_1 != dh_activity.urlid_2)
    # finding the common count between two urlid
    dh_activity = dh_activity.groupby(
        ['urlid_1', 'urlid_2']).agg({user_id: 'count'})
    dh_activity = dh_activity.withColumnRenamed(
        'count(' + user_id + ')', 'common_count')
    # filtering self edges
    # dh_activity = dh_activity.filter(
    #     dh_activity.urlid_1 != dh_activity.urlid_2)
    dh_activity = dh_activity.filter(dh_activity.common_count != 0)
    return dh_activity


def find_score_for_edge(graph_df, output_dataframe_bool=True):
    '''This function will normalize the edge weights of graph.

    Keyword arguments:
    graph_df -- pyspark dataframe containing edges as rows.
    output_dataframe_bool -- indicates whether we want output as dataframe or dictionary.
    
    Output: Spark or pandas dataframe containing edges as row with normalized weights.
    '''
    # finding the total of common count connected to all edges
    all_connections_df = evaluate_all_connections_count_df(graph_df)
    # evaluation edge scores
    edge_score_df = graph_df.join(
        all_connections_df, on='urlid_1', how='inner')
    edge_score_df = edge_score_df.withColumn('edge_score', edge_score_df.common_count /
                                             edge_score_df.total_count). \
        select('urlid_1', 'urlid_2', 'edge_score').drop_duplicates()
    if (output_dataframe_bool):
        # returning dataframe
        return edge_score_df
    else:
        # storing it in the dictionary
        output_dict = edge_score_df.toPandas().set_index(
            ['urlid_1', 'urlid_2'])['edge_score'].dict()
        # return edge score dictionary
        return output_dict


def evaluate_all_connections_count_df(graph_df):
    '''this function will return the sum of edge weights for edges outgoing from each vertex.
    this function is internal to find_score_for_an_edge function.

    Keyword arguments:
    graph_df -- pyspark dataframe containing edges as rows.

    Output -- pyspark dataframe cotaining number of vertices to which a vertex is connected.
    '''
    # grouping by urlid_1 and finding the sum
    graph_df = graph_df.groupby('urlid_1').agg({'common_count': 'sum'})
    # renaming the column
    graph_df = graph_df.withColumnRenamed('sum(common_count)', 'total_count')
    # returning the total count dataframe
    return graph_df


def generate_affinity_score(graph_df, cutoff_length):
    '''This function will generate affinity scores table between every pair of articles in graph.
    The format of graph_df is urlid_1,urlid_2,edge_score.
    
    Keyword arguments:
    graph_df -- pyspark dataframe containing edges(with weights normalized) as rows.
    cutoff_length -- maximum number(including end verticles) of vertices in paths between two vertices.

    Output -- pyspark dataframe containing affinity score for pair of articles .
    '''

    # paths_output stores the paths while score_out stores the score
    paths_output = graph_df.select(graph_df.urlid_1.alias(
        'start'), graph_df.urlid_2.alias('end'), 'edge_score')
    score_out = paths_output.groupby(
        ['start', 'end']).agg({'edge_score': 'sum'})
    score_out = score_out.withColumnRenamed(
        'sum(edge_score)', 'affinity_score')

    for length_var in range(1, cutoff_length - 1):
        # joining for finding paths
        paths_output = paths_output.select('start', paths_output.end.alias('inter' + str(length_var)), \
                                           paths_output.edge_score.alias('edge_score_prev')).join(graph_df.select(
            graph_df.urlid_1.alias('inter' + str(length_var)), graph_df.urlid_2.alias('end'), \
            graph_df.edge_score.alias('edge_score_new')), on='inter' + str(length_var), how='inner')
        # filtering
        paths_output = paths_output.filter(
            paths_output.end != paths_output.start)
        # filtering cyclic paths finished
        for i in range(1, length_var + 1):
            paths_output = paths_output.filter(
                paths_output.end != paths_output['inter' + str(i)])
        # finding score
        paths_output = paths_output.withColumn(
            'edge_score', paths_output.edge_score_prev * paths_output.edge_score_new)
        # selecting essential fields
        paths_output = paths_output.select('start', 'end', 'edge_score')
        # finding score
        score_df = paths_output.groupby(
            ['start', 'end']).agg({'edge_score': 'sum'})
        score_df = score_df.withColumnRenamed(
            'sum(edge_score)', 'affinity_score')
        score_df = score_df.withColumn(
            'affinity_score', score_df.affinity_score * (gamma ** length_var))
        score_out = score_out.union(score_df)

    # final groupby
    score_out = score_out.groupby(
        ['start', 'end']).agg({'affinity_score': 'sum'})
    score_out = score_out.withColumnRenamed(
        'sum(affinity_score)', 'affinity_score')
    score_out = score_out.withColumnRenamed('start', id_x)
    score_out = score_out.withColumnRenamed('end', id_y)
    return score_out


def filter_score_table(graph_df, relevant_urlid, id_y):
    """Function will filter score table.

    Keyword arguments:
    graph_df -- pyspark dataframe containing score for pair of article.
    relevant_urlid -- list of relevant urlids.
    id_y -- name of the column on which filtering will happen.

    Output -- pyspark dataframe after filtering.
    """

    # graph_df = graph_df[graph_df.urlid_x.isin(most_popular_articles_list)]
    graph_df = graph_df[graph_df[id_y].isin(relevant_urlid)]
    return graph_df


def define_networkxgraph(graph_df):
    """Function will define netrorkx graph for calculations.

    Keyword arguments:
    graph_df -- pyspark dataframe containing edges as row.

    Output -- networkx graph object.
    """

    G = nx.Graph()
    list_input_graph = graph_df.toPandas().values.tolist()
    G.add_weighted_edges_from(list_input_graph)
    return G


def find_eigen_vector_centrality_score(G, id_y):
    """Function will find eigen vector centrality score for vertices in graph.

    Keyword arguments:
    G -- networkx graph object.
    id_y -- name of the column used to create pyspark dataframe after score is calculated.

    Output -- pyspark dataframe containing eigen centrality score for vertices in graph.
    """

    eigen_centrality = nx.eigenvector_centrality(G)
    eigen_centrality_df = pd.DataFrame(eigen_centrality.items(), columns=[
        id_y, 'eigen_score'])
    return spark.createDataFrame(eigen_centrality_df)


def find_degree_centrality_score(G, id_y):
    '''This function will find degree centrality score for vertices in graph.

    Keyword arguments:
    G -- networkx graph object.
    id_y -- name of the column used to create pyspark dataframe after score is calculated.

    Output -- pyspark dataframe containing degree centrality score for vertices in graph.
    '''

    degree_central = nx.degree_centrality(G)
    degree_centrality_df = pd.DataFrame(degree_central.items(), columns=[
        id_y, 'degree_score'])
    return spark.createDataFrame(degree_centrality_df)


def merge_scores(affinity_score_df, degree_centrality_df, eigen_centrality_df, id_x, id_y, affinity_score):
    """Function will merge eigen and degree centrality scores with affinity scores.

    Keyword arguments:
    affinity_score_df -- pyspark dataframe containing affinity score between a pair of articles(vertices).
    degree_centrality_df -- pyspark dataframe containing degree centrality score for vertices.
    eigen_centrality_df -- pyspark dataframe containing eigen vector centrality score for vertices.
    id_x -- name of a column in affinity_score_df
    id_y -- name of a column in affinity_score_df
    afiinity_score -- name of the column having affinity score
    """

    # affinity_score = affinity_score.toPandas()
    centrality_score = degree_centrality_df.join(
        eigen_centrality_df, on=id_y, how='inner')

    centrality_score = centrality_score.withColumn(
        'total_centrality_score', centrality_score.eigen_score * centrality_score.degree_score)

    affinity_score_df = affinity_score_df.join(
        centrality_score, on=id_y, how='inner')

    affinity_score_df = affinity_score_df.withColumn(
        "score_pair", affinity_score_df.total_centrality_score * affinity_score_df.affinity_score)
    # final_score_matrix['score_pair'] = final_score_matrix.total_centrality_score * final_score_matrix.affinity_score
    affinity_score_df = affinity_score_df[[id_x, id_y, affinity_score]]
    return affinity_score_df


def remove_random_activity(dh_activity, df_feedback, is_click_feedback, \
                           reco_flag_feedback, random_based, user_id, timestamp, eventdttm_feedback):
    '''This function will remove randomly clicked activity from activity file.

    Keyword arguments:
    dh_activity -- pyspark dataframe for activity.
    df_feedback -- pyspark dataframe for feedback.
    is_click_feedback -- name of the column in feedback representing if an article was clicked or not.
    reco_flag_feedback -- name of the column in feedback containing reco flag for the activity.
    random_based = value of reco_flag for random recommendation.
    user_id -- name of the column representing user's id.
    timestamp -- name of the column containing timestamp in activity file.
    eventdttm_feedback --  name of the column containing timestamp in feedback file.
    '''

    df_feedback_random_clicks = df_feedback.filter((df_feedback[is_click_feedback] == True) \
                                                   & (df_feedback[reco_flag_feedback] == random_based))
    dh_activity = convert_string_datetime_to_yyyymmdd(dh_activity, timestamp, 'date')
    df_feedback_random_clicks = convert_string_datetime_to_yyyymmdd(df_feedback_random_clicks, \
                                                                    eventdttm_feedback, 'date')
    dh_activity = remove_common_rows(spark, dh_activity, df_feedback_random_clicks, user_id, \
                                     'date')
    return dh_activity.drop('date')


def main():
    # load activity and generate urlid list
    dh_activity, relevant_urlid, non_rc_approved_articles = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)

    # load feedback file to remove random activity
    # df_feedback = load_file_and_select_columns(path_feedback,sepr,cols_feedback,spark)
    # df_feedback = find_latest_n_days_activity_data(df_feedback,days_to_consider,eventdttm_feedback)
    # remove random activity using feedback file
    # dh_activity = remove_random_activity(dh_activity,df_feedback,is_click_feedback,reco_flag_feedback,\
    #             random_based,user_id,timestamp,eventdttm_feedback)

    approved_and_popular_article_list = find_approved_and_most_popular_article_list(
        dh_activity, max_num_articles, relevant_urlid, user_id, article_id, non_rc_approved_articles)

    approved_and_popular_article_list = map(
        int, approved_and_popular_article_list)

    dh_activity = dh_activity[dh_activity[article_id].isin(
        approved_and_popular_article_list)]

    graph_df = pair_wise_occurance(dh_activity, user_id, article_id)
    print "pairwise occurance evaluated"

    G = define_networkxgraph(graph_df)
    print "graph defined"

    graph_df = find_score_for_edge(graph_df, True)
    print "edge score evaluated"

    graph_df = generate_affinity_score(graph_df, cutoff_length)
    print "affinity score generated"

    eigen_centrality_df = find_eigen_vector_centrality_score(G, id_y)
    degree_centrality_df = find_degree_centrality_score(G, id_y)
    print "centrality scores generated"

    graph_df = merge_scores(
        graph_df, degree_centrality_df, eigen_centrality_df, id_x, id_y, affinity_score)
    print "scores merged"

    graph_df = filter_score_table(graph_df, relevant_urlid, id_y)
    print "tables filtered"
    # graph_df = subset_by_score_rank(
    #     graph_df, id_x, affinity_score, rank_col, top_n_ss)
    pyspark_df_to_disk(graph_df, temp_path_graph_table, sepr_graph_matrix)
    combine_pyspark_files(
        temp_path_graph_table, path_score_matrix, graph_df.columns, sepr_graph_matrix)

    # save_to_disk(graph_df, path_score_matrix, sepr_graph_matrix)
    print "files saved to disk"


if __name__ == '__main__':
    main()
