#!/home/ubuntu/anaconda/bin/ipython
"""Script will find the cattopic similarity score between pair of articles. It will also find tags for sentiment of headlines"""
#############################################################################

import pandas as pd
from utils import *
from config import *
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize

# sentiment analyser object
nlp = SentimentIntensityAnalyzer()


def headline_sentiment_for_article(title):
    '''This function calculates the sentiment of the arguement(title) and returns the compound sentiment score'''
    score_dict = nlp.polarity_scores(title)
    compound = score_dict['compound']
    return compound


def topic_id_clean(row):
    '''This function will clean the cattopic id column and return list of cattopic ids'''
    topic_id = set(row.split(','))
    try:
        topic_id.remove('')
        return topic_id
    except:
        return topic_id


def cattopic_similarity(merged_df):
    '''This function will return jaccard similarity score between pairs of articles'''
    score_ids_list = []
    active_topic_dict = merged_df.set_index(id_x)[cattopicid_x].to_dict()
    approved_topic_dict = merged_df.set_index(id_y)[cattopicid_y].to_dict()
    for tbid_x in active_topic_dict:
        for tbid_y in approved_topic_dict:
            cattopic_score = len(active_topic_dict[tbid_x] & approved_topic_dict[tbid_y]) / float(
                len(active_topic_dict[tbid_x] | approved_topic_dict[tbid_y]))
            temp = [tbid_x, tbid_y, cattopic_score]
            score_ids_list.append(temp)
    score_ids = pd.DataFrame(score_ids_list, columns=[id_x, id_y, col_cattopic_score])
    return score_ids


def merge_dataframes(meta_clicked, meta_approved):
    # create keys and merge two metadatas
    meta_clicked['key'] = 0
    meta_approved['key'] = 0
    meta_clicked = meta_clicked.rename(columns={article_id: id_x})
    meta_clicked = meta_clicked.drop([approved_bool, non_rc_meta_title], axis=1)
    meta_approved = meta_approved.drop([approved_bool, non_rc_meta_title], axis=1)
    meta_approved = meta_approved.rename(columns={article_id: id_y})
    merged_df = pd.merge(meta_clicked, meta_approved, how='outer', on='key')
    # drop unnecessary columns
    merged_df = merged_df.drop('key', axis=1)
    return merged_df


def tag_sentiment(compound_score):
    '''This function will return the sentiment tag given a compound sentiment score for an article'''
    if compound_score > 0.1:
        return 1
    elif compound_score < 0.1 and compound_score > -0.1:
        return 0
    else:
        return -1


def get_headline_sentiment_for_corpus(non_rc_metadata, rc_metadata):
    '''This function will calculate compound sentiment score for headlines of every articles in corpus'''
    # subset metadata
    non_rc_metadata_1 = non_rc_metadata[[article_id, non_rc_meta_title]]
    # drop articles with no title
    rc_metadata.dropna(subset=[rc_meta_title], axis=0, how='any', inplace=True)
    non_rc_metadata_1.dropna(subset=[non_rc_meta_title], axis=0, how='any', inplace=True)

    # merge two metadata
    rc_metadata = rc_metadata.rename(columns={rc_meta_title: non_rc_meta_title})
    metadata = rc_metadata.append(non_rc_metadata_1, ignore_index=True)
    # calculate compound sentiment for all article and drop unnecessary columns
    metadata[col_headline_sentiment] = metadata[non_rc_meta_title].apply(headline_sentiment_for_article)
    metadata = metadata.drop(non_rc_meta_title, axis=1)
    metadata[col_sentiment_tag] = metadata[col_headline_sentiment].apply(tag_sentiment)
    return metadata


def generate_cattopic_similarity_score(path_activity, non_rc_metadata):
    '''This function is the wrapper function to calculate cattopic score between pair of articles'''
    # load files
    df_activity = load_file_and_select_columns(path_activity, sepr=sepr, cols=[article_id, timestamp])
    df_activity = find_latest_n_days_activity_data(df_activity, days_to_consider, timestamp)
    clicked_articles_list = make_unique_col_list(df_activity, article_id)
    # clean cattopic id column in metadata
    non_rc_metadata = non_rc_metadata[~non_rc_metadata[non_rc_cattopicid].isnull()]
    non_rc_metadata[non_rc_cattopicid] = non_rc_metadata[non_rc_cattopicid].apply(topic_id_clean)
    # get metadata of approved articles and clicked articles
    meta_approved = non_rc_metadata[non_rc_metadata[approved_bool] == True]
    meta_clicked = non_rc_metadata[non_rc_metadata[article_id].isin(clicked_articles_list)]
    # drop unnecessary columns
    merged_df = merge_dataframes(meta_clicked, meta_approved)
    # calculate cattopic similarity between pairs(clicked and approved articles)
    cattopic_score = parallelize(merged_df, cattopic_similarity)
    cattopic_score = cattopic_score[cattopic_score[id_x] != cattopic_score[id_y]]
    # return cattopic score
    return cattopic_score


def save_scores(sentiment_df, cattopic_score_df, path_sentiment_score, path_cattopic_score, sepr):
    '''This function will save the cattopic and sentiment dataframes as flat files'''
    sentiment_df.to_csv(path_sentiment_score, sep=sepr, index=False)
    cattopic_score_df.to_csv(path_cattopic_score, sep=sepr, index=False)


def main():
    # load metadata
    rc_metadata = load_file_and_select_columns(path_rc_metadata, sepr=sepr, cols=[article_id, rc_meta_title])
    non_rc_metadata = load_file_and_select_columns(path_metadata, sepr=sepr,
                                                   cols=[article_id, non_rc_meta_title, approved_bool,
                                                         non_rc_cattopicid])
    # sentiment analysis of article headlines
    sentiment_df = get_headline_sentiment_for_corpus(non_rc_metadata, rc_metadata)
    cattopic_score_df = generate_cattopic_similarity_score(path_activity, non_rc_metadata)
    # save both sentiment score and cattopic score
    save_scores(sentiment_df, cattopic_score_df, path_sentiment_score, path_cattopic_score, sepr)


if __name__ == "__main__":
    main()
