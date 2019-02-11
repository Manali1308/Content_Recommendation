#!/home/ubuntu/anaconda/bin/ipython
"""Script will evaluate cosine similarity between every pair of articles using doc2vec."""
#############################################################################
# imports
import pandas as pd
import pickle
from gensim.models import word2vec
import numpy as np
from config import *
from utils import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import gensim
import ast
import datetime as dt

##########################################################################
# spark
conf = SparkConf()
conf.setMaster(
    "local[*]").setAppName('App Name').set("spark.executor.memory", "100g").set("spark.driver.memory", "100g")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.master("local[*]").appName("App Name").config(
    "spark.some.config.option", "some-value", conf).getOrCreate()
print "importing and initialisation finished"


#############################################################################
# function block


def parallelize(data, func):
    """Function will parallelize the processing.

    Keyword arguments:
    data -- pandas dataframe
    func -- name of the function

    Output -- result(pandas dataframe)
    """

    cores = cpu_count()  # Number of CPU cores on your system
    partitions = cores
    data_split = np.array_split(data, partitions)
    pool = Pool(cores)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data


# added by Pragalbh on 10th Jan 2018


def check_int(x):
    """This function will check if the value is a number or not.

    Keyword arguments:
    x -- the value to be checked

    Output -- Boolean specifying whether its a number or not.
    """

    return str(x).isdigit()


def clean_preprocessed_file(text_corpus, index_column_name):
    """This function will convert string article ids to integer article ids.

    Keyword arguments:
    text_corpus -- pandas dataframe for text corpus
    index_column_name -- name of the column containing ids of articles(string)
    """

    text_corpus = text_corpus[text_corpus[index_column_name].apply(check_int)]
    text_corpus[index_column_name] = text_corpus[
        index_column_name].astype('int')
    return text_corpus


def refresh_article_list(text_corpus, index_column_name, path_preprocessed_article_list):
    """Function will update already_processed_article_list and will save it to disk.

    Keyword arguments:
    text_corpus -- pandas dataframe for text corpus
    index_column_name -- name of the column containing ids of articles(string)
    path_preprocessed_article_list -- path where list of preprocessed articles are saved(string)
    """

    preprocessed_article_list = list(text_corpus[index_column_name].unique())
    print 'no. of preprocessed articles= ' + str(len(preprocessed_article_list))
    try:
        save_list_as_pickle(preprocessed_article_list, path_preprocessed_article_list)
    except Exception as e:
        print e


def load_preprocessed_corpus(path_preprocessed_files):
    """Function will load preprocessed corpus.

    Keyword arguments:
    path_preprocessed_article_list -- path where list of preprocessed articles are saved(string)

    Output -- Pandas dataframe for text corpus
    """

    text_corpus = load_file_and_select_columns(path_preprocessed_files, sepr)
    # text_corpus = pd.read_csv(path_preprocessed_files, sep=sepr)
    text_corpus = text_corpus.dropna()
    text_corpus = text_corpus.drop_duplicates()
    # added by pragalbh on 10th Jan 2018
    text_corpus = clean_preprocessed_file(text_corpus, index_column_name)
    # new_article_list = pickle.load(open(path_new_article_list, "rb"))
    return text_corpus


def define_word2vec_model(word2vec_params):
    """Function will define the model object.

    Keyword arguments:
    word2vec_params -- dictionary of model parameter and values.
    """

    model = word2vec.Word2Vec(
        iter=word2vec_params['iter'], size=word2vec_params['size'], window=word2vec_params['window'],
        min_count=word2vec_params['min_count'], workers=word2vec_params['workers'], seed=word2vec_params['seed'])
    return model


# TODO


def train_word2vec_model(text_corpus, model, text_column_name, update=False):
    """Function will train word2vec model object.

    Keyword arguments:
    text_corpus -- pandas dataframe for text corpus.
    model -- model object.
    text_column_name -- name of the column containing text for each article.(string)
    update -- boolean specifying whether to update the model or not.
    """

    if update:  # change added gourav 07/02/18
        model.build_vocab([ast.literal_eval(i) for i in text_corpus[
            text_column_name].tolist()], update=True)
    else:
        model.build_vocab([ast.literal_eval(i)
                           for i in text_corpus[text_column_name].tolist()])
    model.train([ast.literal_eval(i) for i in text_corpus[text_column_name].tolist()],
                total_examples=model.corpus_count, epochs=model.iter)
    return model


def predict_word2vec_vector(model, word):
    """Function will return the word embeddings for a word.

    Keyword arguments:
    model -- model object.
    word -- word(string).

    Output -- list of embeddings for the word.
    """

    return model.wv[word]


def doc2vec_on_document(text_vector):
    """Function will find doc2vec representation of a document.

    Keyword arguments:
    text_vector -- string representation of list of words in a document

    Output -- doc2vec list for the document
    """

    global model
    # start change added gourav 07/02/18
    text_vector = ast.literal_eval(text_vector)
    doc2vec = np.array(np.zeros(model.vector_size))
    common_words = set(text_vector) & set(model.wv.vocab.keys())
    # end change added gourav 07/02/18
    for word in set(common_words):
        doc2vec += predict_word2vec_vector(model, word)
    doc2vec = doc2vec / len(set(common_words))
    return doc2vec


def doc2vec_on_corpus(text_corpus):
    """Function will apply doc2vec on entire corpus.

    Keyword arguments:
    text_corpus -- pandas dataframe for text corpus

    Output -- pandas dataframe for doc2vec representation for all documents
    """

    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(doc2vec_on_document)
    return text_corpus


def cosine_similarity(vector_1, vector_2):
    """Function will evaluate cosine similarity between two vectors.

    Keyword arguments:
    vector_1 -- 1st vector(list).
    vector_2 -- 2nd vector(list).

    Output -- cosine similarity score betweeen two vectors(float).
    """

    similarity_score = np.dot(vector_1, vector_2) / \
                       (np.linalg.norm(vector_1) * np.linalg.norm(vector_2))
    return similarity_score


# TODO


def mod_vectors(text_corpus):
    '''This function will find the modulus of all doc2vec vectors for all
     documents in corpus.

    Keyword arguments:
    text_corpus -- pandas dataframe for the text corpus.

    Output -- dictionary containing modulus of each doc2vec vector.
    '''
    dict_mod = {}
    id_list = list(text_corpus[index_column_name].unique())
    vectors_list = list(text_corpus[text_column_name])
    for i in range(len(id_list)):
        dict_mod.update({id_list[i]: np.linalg.norm(vectors_list[i])})
    return dict_mod


def cosine_similarity_for_corpus(text_corpus, dict_mod, relevant_article_id):
    """Function will evaluate cosine similarity for every document in the corpus.

    Keyword arguments:
    text_corpus -- pandas dataframe for text corpus.
    dict_mod -- dictionary containing modulus of each article's doc2vec representation.
    relevant_article_id -- list of all approved articles.
    """

    id_list = list(text_corpus[index_column_name].unique())
    relevant_article_id = list(set(relevant_article_id) & set(id_list))
    print 'no of articles in text corpus= ' + str(len(id_list))
    # vectors_list = list(text_corpus[text_column_name])
    score_ids_list = []
    dict_corpus = text_corpus.set_index(index_column_name)[
        text_column_name].to_dict()
    print 'no of relevant articles = ' + str(len(relevant_article_id))
    for ids in id_list:
        for rel_id in relevant_article_id:
            score = np.dot(dict_corpus[ids], dict_corpus[
                rel_id]) / (dict_mod[ids] * dict_mod[rel_id])
            temp = [ids, rel_id, score]
            score_ids_list.append(temp)
    corpus_similarity_score = pd.DataFrame(
        score_ids_list, columns=[id_x, id_y, ss_score_col])
    return corpus_similarity_score


def save_cosine_similarity(corpus_similarity_score, path_content_score_matrix):
    """Function will save cosine similarity for corpus to disk.
    
    Keyword arguments:
    corpus_similarity_score -- pandas dataframe for content similarity score matrix.
    path_content_score_matrix -- path of storage for content similarity score matrix(string).
    """

    # temp_df = corpus_similarity_score[
    #     [id_y, id_x, ss_score_col]]
    # temp_df.columns = [id_x, id_y, ss_score_col]
    # corpus_similarity_score = corpus_similarity_score.append(temp_df)
    corpus_similarity_score.to_csv(
        path_content_score_matrix, index=False, sep=sepr)


def save_word2vec_model(model, path):
    '''Function will save word2vec model

    Keyword arguments:
    model -- word2vec model object.
    path -- path where model will be saved.(string)
    '''

    model.save(path)


def load_word2vec_model(path):
    '''Function will load pre-trained word2vec model.

    Keyword arguments:
    path -- path where model is saved.

    Output -- model object
    '''

    model = gensim.models.Word2Vec.load(path)
    return model


def finding_new_articles_to_train(text_corpus, path_preprocessed_article_list):
    '''This function will find new articles to retrain/train the model.

    Keyword arguments:
    text_corpus -- pandas dataframe for text corpus.
    path_preprocessed_article_list -- path where preprocessed article list is saved(string).

    Output -- pandas dataframe containing articles on which training/retraining will happen.
    '''

    try:
        article_id_list = pickle.load(
            open(path_preprocessed_article_list, "rb"))
        text_corpus = text_corpus[~text_corpus[
            index_column_name].isin(article_id_list)]
    except Exception as e:
        print e
    return text_corpus


def main():
    """Function will perform all the calls."""
    global text_corpus, model, relevant_article_id, text_column_name
    try:
        print 'enter try'
        model = load_word2vec_model(path_word2vec_model)
        print 'pre-trained model loaded'
        new_text = finding_new_articles_to_train(
            text_corpus, path_preprocessed_article_list)
        print 'no. of new articles= ' + str(new_text.shape[0])
        if new_text.shape[0] != 0:
            model = train_word2vec_model(
                new_text, model, text_column_name, update=True)
            print 'word2vec model trained for new articles'
            refresh_article_list(text_corpus, index_column_name, path_preprocessed_article_list)
            print 'preprocessed article list updated'
        save_word2vec_model(model, path_word2vec_model)
    except Exception as e:
        print e
        model = define_word2vec_model(word2vec_params)
        print 'model defiend'
        model = train_word2vec_model(
            text_corpus, model, text_column_name, update=False)
        print 'word2vec model trained'
        save_word2vec_model(model, path_word2vec_model)
        print 'model saved'
        refresh_article_list(text_corpus, index_column_name, path_preprocessed_article_list)
        print 'preprocessed articles list refreshed'

    dh_activity, _, _ = file_loader(
        spark, path_activity, cols_activity_graph, sepr,
        article_id, path_metadata, cols_metadata, path_approved_links,
        cols_approved_links, approved_bool, days_to_consider)

    small_article_list = list(relevant_article_id)
    seen_articles = make_unique_col_list(dh_activity, article_id)
    del dh_activity
    small_article_list.extend(seen_articles)
    text_corpus = text_corpus[text_corpus[
        index_column_name].isin(small_article_list)]
    # text_corpus = doc2vec_on_corpus(text_corpus, text_column_name)
    text_corpus = parallelize(text_corpus, doc2vec_on_corpus)
    print 'doc2vec representations formed'
    # TODO: normalise doc2vec
    dict_norm = mod_vectors(text_corpus)
    print 'dictionary of modulus formed'
    # print len(relevant_article_id)
    # print len(text_corpus[index_column_name].tolist())

    relevant_article_id = list(
        set(text_corpus[index_column_name].tolist()) & set(relevant_article_id))
    print 'relevant ids are ' + str(len(relevant_article_id))

    corpus_similarity_score = cosine_similarity_for_corpus(
        text_corpus, dict_norm, relevant_article_id)
    print 'similarity score generated'
    # added 09/03 to remove articles which are exactly similar
    corpus_similarity_score = corpus_similarity_score[corpus_similarity_score[ss_score_col] < 0.9]
    # added march 16
    # corpus_similarity_score
    # =subset_by_score_rank(corpus_similarity_score,id_x,ss_score_col,ss_rank_col,top_n_ss)

    save_cosine_similarity(corpus_similarity_score, path_content_score_matrix)
    print 'content based similarity scores generated & saved to mentioned path '


if __name__ == "__main__":
    text_corpus = load_preprocessed_corpus(path_preprocessed_files)
    print 'corpus loaded'
    # drop duplicates from text_corpus
    text_corpus = text_corpus.drop_duplicates(article_id)
    # model = define_word2vec_model(word2vec_params)
    # print 'model defiend'
    dh_metadata = load_metadata(
        spark, path_metadata, cols_metadata, sepr)

    df_approved_links = load_file_and_select_columns(path_approved_links, sepr)
    rc_approved_articles = list(df_approved_links[article_id].unique())
    relevant_article_id = find_approved_article_list(
        dh_metadata, approved_bool, article_id)

    relevant_article_id.extend(rc_approved_articles)
    print 'relevant articles loaded'
    print 'no of relevant articles = ' + str(len(relevant_article_id))
    save_list_as_pickle(relevant_article_id, path_relevant_article_ids)
    print 'relevant article list saved'
    main()
