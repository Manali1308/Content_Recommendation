#!/home/ubuntu/anaconda/bin/ipython
"""Script will clean and do preprocessing of text corpus for evaluating text similarity."""
#############################################################################
# imports
import pandas as pd
import pickle
import re
from bs4 import BeautifulSoup
from nltk.tokenize import TreebankWordTokenizer
from string import punctuation
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
from nltk.corpus import stopwords
from config import *
import datetime as dt
from utils import *
import nltk

nltk.download('stopwords')
nltk.download('wordnet')
#############################################################################
# variable block

# globals
cleanr = re.compile('<.*?>')
cleann = re.compile('\n')
cleans = re.compile('\s+')

replacement_patterns = [(r'can\'t', 'cannot'), (r'won\'t', 'will not'), (r'i\'m', 'i am'), (r'ain\'t', 'is not'),
                        (r'(\w+)\'ll', '\g<1> will'),
                        (r'(\w+)n\'t', '\g<1> not'), (r'(\w+)\'ve', '\g<1> have'), (r'(\w+)\'s', '\g<1> is'),
                        (r'(\w+)\'re', '\g<1> are'), (r'(\w+)\'d', '\g<1> would')]

patterns = [(re.compile(regex), repl)
            for (regex, repl) in replacement_patterns]

tokenizer_tree = TreebankWordTokenizer()
lemmatizer = WordNetLemmatizer()


#############################################################################


def find_new_articles(text_corpus, index_column_name, already_processed_article_list):
    """Function will find new articles. Will save the list to disk.

    Keyword arguments:
    text_corpus -- Pandas dataframe representing text corpus.
    index_column_name -- name of the column containing id of an article.
    already_preprocessed_article_list -- list of articles which are already cleaned.
    
    Output -- list of articles which are not cleaned.
    """

    all_article_list = list(text_corpus[index_column_name].unique())
    not_preprocessed_article_list = list(
        set(all_article_list) - set(already_processed_article_list))
    # pickle.dump(not_preprocessed_article_list,
    #             open(path_new_article_list, "wb"))
    return not_preprocessed_article_list


def subset_corpus(text_corpus, index_column_name, not_preprocessed_article_list):
    """Function will subset the whole corpus for new articles.

        Keyword arguments:
        text_corpus -- Pandas dataframe represeting text corpus.
        index_column_name -- name of the column containing id of an article.
        not_preprocessed_article_list -- list of articles which are not cleaned.

        Output -- pandas dataframe containing only those articles which were
                  not cleaned(or we can say, are the new articles).
    """

    text_corpus = text_corpus[text_corpus[
        index_column_name].isin(not_preprocessed_article_list)]
    return text_corpus


def convert_corpus_to_list(text_corpus):
    """Function will convert corpus from dataframe to list of list type.

        Keyword arguments:
        text_corpus -- pandas dataframe representing text corpus.

        Output -- list of list representation of text corpus.
    """

    text_corpus = text_corpus.values.tolist()
    return text_corpus


def remove_tags(text):
    """Function will take text(string) as input and will remove HTLM tags and
    tables.

    Keyword arguments:
    text -- text(string).

    Output -- text(string) after cleaning.
    """

    global cleanr
    global cleann
    global cleans
    try:
        text = BeautifulSoup(text)
        for table in text.findAll("table"):
            table.extract()
        text = text.text
        text = re.sub(cleanr, '', text)
        text = re.sub(cleann, '', text)
        text = re.sub(cleans, ' ', text)

    except Exception as e:
        pass

    return text


def contraction_expansion(text):
    """Function will expand the contractions for a text.

    Keyword arguements:
    text -- text(string)

    Output -- text(string) after cleaning.
    """

    global patterns
    for (pattern, repl) in patterns:
        (text, _) = re.subn(pattern, repl, text)
    return text


def tokenization(text):
    """Function will tokenize text vector and will return text_vector.

    Keyword arguements:
    text -- text(string)

    Output -- list of tokens in text.
    """

    global tokenizer_tree
    tokenised_document = tokenizer_tree.tokenize(text)
    return tokenised_document


def punctutation_removal(tokenised_document):
    """Function will remove punctutations from the text_vector.

    Keyword arguments:
    tokenized_document -- list of tokens in an article.

    Output -- list of tokens with punctuations removed.
    """

    tokenised_document = [
        i for i in tokenised_document if i not in punctuation]
    tokenised_document_cleaner = []
    for i in tokenised_document:
        word = re.split(r'\W', i)
        if (len(word) == 1):
            tokenised_document_cleaner.append(word[0])
        elif (len(word) == 2):
            if (word[0].isalpha() == False):
                tokenised_document_cleaner.append(word[1])
            elif (word[1].isalpha() == False):
                tokenised_document_cleaner.append(word[0])
            elif (word[0].isalpha() == True and word[1].isalpha() == True):  # can affect collocations
                tokenised_document_cleaner.append(word[0])
                tokenised_document_cleaner.append(word[1])
    # to remove null strings
    resultant_tokenised_doc = []
    for word in tokenised_document_cleaner:
        if word != '':
            resultant_tokenised_doc.append(word)
    return resultant_tokenised_doc


def lower_case(text_vector):
    """Function will convert the text_vector to lower case.

    Keyword arguments:
    text_vector -- list of tokens in an article.

    Output -- list of tokens after lowercasing.
    """

    tokenised_document = [token.lower() for token in text_vector]
    return tokenised_document


def postag_doc(text_vector):
    """Function will perform pos-tagging.To be used inside lemitization.

    Keyword arguments:
    text_corpus -- list of tokens in an article
    """

    tokenised_document_pos_tagged = pos_tag(text_vector)
    return tokenised_document_pos_tagged


def map_postags(treebank_tag):
    """Function will map output from postag function to what is required by lemitization function.

    Keyword arguments:
    treebank_tag -- 
    """

    if treebank_tag.startswith('J'):
        return "a"
    elif treebank_tag.startswith('V'):
        return "v"
    elif treebank_tag.startswith('N'):
        return "n"
    elif treebank_tag.startswith('R'):
        return "r"
    else:
        return 'n'


def lemitization(text_vector):
    """Function will convert the words to it's basic forms (lemitization).
    
    Keyword arguments:
    text_vector -- list of tokens in an article.

    Output -- list of tokens after lemmatization.
    """

    text_vector = postag_doc(text_vector)
    global lemmatizer
    tokenised_document = [lemmatizer.lemmatize(word, pos=map_postags(
        postag)) for word, postag in text_vector]
    return tokenised_document


def stopwords_removal(text_vector):
    """Function will remove stopwords from text_vector.

    Keyword arguments:
    text_vector -- list of tokens in an article.

    Output -- list of tokens after removing stopwords.
    """

    text_vector = [
        i for i in text_vector if i not in stopwords.words('english')]
    return text_vector


def remove_tags_from_corpus(text_corpus):
    """Function will apply remove_tags function to the whole corpus.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe for text corpus.
    """

    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(remove_tags)
    text_corpus = text_corpus[text_corpus[text_column_name] != ' ']
    return text_corpus


def contraction_expansion_on_corpus(text_corpus):
    """Function will perform contraction expansion on entire corpus.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe for text corpus.
    """

    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(contraction_expansion)
    return text_corpus


def tokenization_on_corpus(text_corpus):
    """Function will perform tokenization on entire corpus.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe for text corpus.
    """

    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(tokenization)
    return text_corpus


def punctuation_removal_on_corpus(text_corpus):
    """Function will perform punctutaion removal on entire corpus.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe for text corpus.
    """
    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(punctutation_removal)
    return text_corpus


def lowercase_on_corpus(text_corpus):
    """Function will convert the docmument to lowercase on entire corpus.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe for text corpus.
    """

    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(lower_case)
    return text_corpus


def lemmitization_on_corpus(text_corpus):
    """Function will lemmitize each document in the corpus.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe for text corpus.
    """

    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(lemitization)
    return text_corpus


def stopword_removal_on_corpus(text_corpus):
    """Function will remove the stopwords from the corpus.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe for text corpus.
    """

    text_corpus[text_column_name] = text_corpus[
        text_column_name].apply(stopwords_removal)
    return text_corpus


def store_to_disk(text_corpus, path_preprocessed_files, append_mode=True):
    """Function will save the files to the disk.
    It will append to already stored preprocessed file.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.
    path_preprocessed_files -- path of the preprocessed files.
    """

    if append_mode:
        text_corpus.to_csv(path_preprocessed_files, sep='|',
                           index=False, mode='a', header=False)
    else:
        text_corpus.to_csv(path_preprocessed_files, sep='|',
                           index=False, header=True)


def import_data_and_subset(path_metadata, path_rc_metadata, sepr, path_preprocessed_article_list, article_id,
                           text_column_name, rc_content_column_name, reset_already_preprocessed_article_list=False):
    """Function will take the path of the text file as in
    input and will return a dataframe with required columns only.
    It will also load already_processed_article_list and will create an empty if not present.
    It can also reset already_processed_article_list.

    Keyword arguments:
    path_metadata -- path of the non rc metadata file.
    path_rc_metadata -- path of the rc metadata file.
    sepr -- seperator(string).
    path_preprocessed_article_list -- path of the list of preprocessed articles.
    article_id -- name of the column containing id of the article.(string)
    text_column_name -- name of the column containing text of the non-rc article.(string)
    rc_content_column_name -- name of the column containing text in rc text corpus.
    reset_already_preprocessed_article_list -- boolean representing whether to reset the list of 
                                                already preprocessed articles or not.

    """

    non_rc_text_corpus = load_file_and_select_columns(path_metadata, sepr)
    non_rc_text_corpus = non_rc_text_corpus[[article_id, text_column_name]]

    rc_text_corpus = pd.read_csv(path_rc_metadata, sep=sepr)
    rc_text_corpus = rc_text_corpus[[article_id, rc_content_column_name]]
    rc_text_corpus = rc_text_corpus.rename(
        columns={rc_content_column_name: text_column_name})

    text_corpus = non_rc_text_corpus.append(rc_text_corpus)
    # text_corpus = text_corpus[(~text_corpus[expdate].isnull()) & (
    #     ~text_corpus[text_column_name].isnull())]
    # text_corpus = text_corpus.drop_duplicates()
    # text_corpus[expdate] = pd.to_datetime(text_corpus[expdate])
    # filter_date = dt.date.today() - dt.timedelta(days=days_to_consider)
    # text_corpus = text_corpus[text_corpus[expdate] > filter_date]
    text_corpus = text_corpus.dropna()
    text_corpus = text_corpus.drop_duplicates()
    if reset_already_preprocessed_article_list:
        already_processed_article_list = []
        save_list_as_pickle(already_processed_article_list, path_preprocessed_article_list)
    else:
        try:
            already_processed_article_list = pickle.load(
                open(path_preprocessed_article_list, "rb"))
        except Exception as e:
            print "already propressed list doens't exists"
            already_processed_article_list = []
    return text_corpus, already_processed_article_list


def find_number_of_tokens(text):
    '''This function will find the number of tokens in a list.

    Keyword arguments:
    text -- list of tokens

    Output --  length of list of tokens
    '''

    return len(text)


def remove_article_with_little_or_no_content(text_corpus):
    '''This function will remove articles from text corpus which have less than 5 tokens after preprocessing.

    Keyword arguments:
    text_corpus -- Pandas dataframe for text corpus.

    Output -- Pandas dataframe after removing articles with too little or no content.
    '''

    text_corpus['tokens'] = text_corpus[text_column_name].apply(find_number_of_tokens)
    text_corpus = text_corpus[text_corpus['tokens'] >= 15]
    text_corpus = text_corpus.drop('tokens', axis=1)
    return text_corpus


def main():
    """main function to perform all the cleaning."""

    global text_column_name
    # set to false
    # TODO: add RC corpus to non RC corpus
    text_corpus, already_processed_article_list = import_data_and_subset(
        path_metadata, path_rc_metadata, sepr, path_preprocessed_article_list, article_id, text_column_name,
        rc_content_column_name, reset_already_preprocessed_article_list=False)

    print 'raw corpus loaded'

    # text_corpus = remove_tags_from_corpus(
    #     text_corpus, text_column_name)

    not_preprocessed_article_list = find_new_articles(
        text_corpus, index_column_name, already_processed_article_list)
    print 'new_article list is ' + str(len(not_preprocessed_article_list))

    text_corpus = subset_corpus(
        text_corpus, index_column_name, not_preprocessed_article_list)
    print 'subsetting of unprocessed articles from corpus done'
    print 'articles left after subsetting are ' + str(text_corpus[index_column_name].nunique())

    text_corpus = parallelize(text_corpus, remove_tags_from_corpus)
    print 'html tags removed from corpus'
    # text_corpus = convert_corpus_to_list(text_corpus)

    # text_corpus = contraction_expansion_on_corpus(
    #     text_corpus, text_column_name)
    text_corpus = parallelize(text_corpus, contraction_expansion_on_corpus)
    print 'contractions expansion completed'

    # text_corpus = tokenization_on_corpus(text_corpus, text_column_name)
    text_corpus = parallelize(text_corpus, tokenization_on_corpus)
    print 'tokenization completed'
    # text_corpus = punctuation_removal_on_corpus(text_corpus, text_column_name)
    text_corpus = parallelize(text_corpus, punctuation_removal_on_corpus)
    print 'punctuation removal completed'
    # text_corpus = lowercase_on_corpus(text_corpus, text_column_name)
    text_corpus = parallelize(text_corpus, lowercase_on_corpus)
    print 'lower casing done'
    # text_corpus = lemmitization_on_corpus(text_corpus, text_column_name)
    text_corpus = parallelize(text_corpus, lemmitization_on_corpus)
    print 'lemmatization done'
    # text_corpus = stopword_removal_on_corpus(text_corpus, text_column_name)
    text_corpus = parallelize(text_corpus, stopword_removal_on_corpus)
    print 'stopwords removed'
    text_corpus = remove_article_with_little_or_no_content(text_corpus)
    print 'articles with less or no corpus removed'
    store_to_disk(text_corpus, path_preprocessed_files, append_mode=True)
    print 'per-processing done and saved at mentioned path'


if __name__ == '__main__':
    main()
