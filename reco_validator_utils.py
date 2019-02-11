#!/home/ubuntu/anaconda/bin/ipython
"""Script will contains functions for validating generated recommendations."""
import smtplib
import os
import datetime as dt
import sys
from config import *
from utils import *
import pyspark.sql.functions as F
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders
from pyspark.sql.window import Window
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import subprocess


# import reco_uploader
# from graph_score_generator import file_loader
##########################################################################
# spark configuration
# spark = get_spark_session_object()


def send_mail(body, toaddr=kf_mail_list, attach=True):
    '''this function will send the mail to the developer with the body provided.
    
    Keyword arguments:
    body -- body of the mail.
    toaddr -- list of addresses to whom mail will be sent.
    attach -- boolean specifying whether we want to attach analysis file or not
    '''

    # configure SMTP
    fromaddr = "ehdatascialert@gmail.com"
    # configuring the message
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = ", ".join(toaddr)
    msg['Subject'] = "Recommendation system issue"
    # attach the body to the mail
    msg.attach(MIMEText(body, 'plain'))
    # attach the analysis file to the mail
    if attach:
        filename = reco_analysis_file_name
        try:
            attachment = open(path_reco_analysis, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
            msg.attach(part)
        except:
            pass

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, "ehdatasci123")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()


def analysis_of_reco(rc_reco_df, non_rc_reco_df, speciality_reco_df):
    '''Function will analyse the reco dataframes.

    Keyword arguments:
    rc_reco_df -- pandas dataframe for rc recommendations
    non_rc_reco_df -- pandas dataframe for non rc recommendations
    speciality_reco_df -- pandas dataframe for specialty based recommendaton
    '''

    # find all rc users in reco file
    rc_users = len(make_unique_col_list(rc_reco_df, user_id))
    # find all non rc users in reco file
    non_rc_users = len(make_unique_col_list(non_rc_reco_df, user_id))
    # find all non rc speciality users in reco file
    speciality_reco_df = len(make_unique_col_list(speciality_reco_df, user_id))
    # load rc targets file and find list of all target users
    # df_rc_targets = pd.read_csv(path_rc_targets,sep =sepr)
    df_rc_targets = load_file_and_select_columns(path_rc_targets, sepr)
    num_of_user_in_rc_targets = len(make_unique_col_list(df_rc_targets, user_id))
    # write the analysis to the file
    f = open(path_reco_analysis, "w+")
    f.write("num_of_user_in_rc_targets=" + str(num_of_user_in_rc_targets) + '\n')
    f.write("num of user in Rc recom=" + str(rc_users) + '\n')
    f.write("num of users non Rc recom=" + str(non_rc_users) + '\n')
    f.write("num of user in speciality recom=" + str(speciality_reco_df) + '\n')
    f.close()


def get_reco_path_on_s3(rc_reco_path_on_s3, non_rc_reco_path_on_s3, specialty_reco_path_on_s3):
    '''Function will get reco path on s3.

    Keyword arguments:
    rc_reco_path_on_s3 -- path of rc reco on s3(string)
    non_rc_reco_path_on_s3 -- path of non rc reco on s3(string)
    specialty_reco_path_on_s3 -- path of specialty reco on s3(string)
    '''

    current_date = dt.datetime.today().strftime('%Y%m%d')
    print 'current date is ' + current_date
    rc_reco_path_on_s3_complete = rc_reco_path_on_s3 + current_date + \
                                  '/rc_recommendations_' + current_date + '.csv'
    print rc_reco_path_on_s3_complete
    non_rc_reco_path_on_s3_complete = non_rc_reco_path_on_s3 + current_date + \
                                      '/non_rc_recommendations_' + current_date + '.csv'
    print non_rc_reco_path_on_s3_complete
    specialty_reco_path_on_s3_complete = specialty_reco_path_on_s3 + \
                                         current_date + '/specialty_recos_' + current_date + '.csv'
    print specialty_reco_path_on_s3_complete
    return rc_reco_path_on_s3_complete, non_rc_reco_path_on_s3_complete, specialty_reco_path_on_s3_complete


def download_file(reco_path_on_s3, path_download_on_instance):
    '''Function will download reco file from bucket to instance.

    Keyword arguments:
    reco_path_on_s3 -- path of reco file on s3(string)
    path_download_on_instance -- path of reco file on instance(string)
    '''

    # create command
    cmd = '/home/ubuntu/anaconda/bin/aws s3 cp ' + 's3://' + bucket_name + reco_path_on_s3 + \
          ' ' + path_download_on_instance
    print cmd
    # run command
    if os.system(cmd) != 0:
        print 'reco file not dowloaded' + path_download_on_instance
        send_mail('Reco files not found', attach=False)


def download_read_reco_file(rc_reco_path_on_s3, non_rc_reco_path_on_s3, specialty_reco_path_on_s3):
    '''wrapper function to download and load reco files.

    Keyword arguments:
    rc_reco_path_on_s3 -- path of rc reco file on s3(string)
    non_rc_reco_path_on_s3 -- path of non rc reco file on s3(string)
    specialty_reco_path_on_s3 -- path of specialty reco file on s3(string)
    '''

    # download reco files
    download_file(rc_reco_path_on_s3, rc_reco_path_download_on_instance)
    download_file(non_rc_reco_path_on_s3, non_rc_reco_path_download_on_instance)
    download_file(specialty_reco_path_on_s3, speciality_path_download_on_instance)
    # load the reco files as dataframe
    # rc_reco_df = pd.read_csv(rc_reco_path_download_on_instance,sep = ',')
    # non_rc_reco_df = pd.read_csv(non_rc_reco_path_download_on_instance,sep = ',')
    # speciality_reco_df = pd.read_csv(speciality_path_download_on_instance,sep = ',')
    # load the reco files as dataframe
    rc_reco_df = load_file_and_select_columns(rc_reco_path_download_on_instance, sepr=',')
    non_rc_reco_df = load_file_and_select_columns(non_rc_reco_path_download_on_instance, sepr=',')
    speciality_reco_df = load_file_and_select_columns(speciality_path_download_on_instance, sepr=',')
    # return
    return rc_reco_df, non_rc_reco_df, speciality_reco_df


def is_article_id_from_metadata(df_appr, reco_df):
    '''Function will check if the article id in reco file are from metadata or not.

    Keyword arguements:
    df_appr -- pandas dataframe containing metadata for approved articles.
    reco_df -- pandas dataframe for recommendations.

    Output --  boolean specifying if all articles in reco file are from metadata or not.
    '''
    not_present_df = reco_df[~reco_df.TBID.isin(df_appr.TBID)]
    return len(not_present_df) == 0


def find_user_not_sent_reco(df_reco, df_user):
    '''Function will find number of users not sent reco.

    Keyword arguments:
    df_reco -- pandas dataframe for recommendations.
    df_user -- pandas dataframe for target users.

    Output -- number of users not sent reco.
    '''
    reco_user_list = df_reco[user_id].unique().tolist()
    total_user = df_user[user_id].unique().tolist()
    not_sent = len(set(total_user) - set(reco_user_list))
    return not_sent


def error_in_log():
    '''This function will find if there is error in log file and return the error if present.

    Output:
    1. Boolean specifying if the error message was present in log file or not.
    2. Error message if error was present(else empty string).
    '''

    cmd_1 = "grep 'Error' " + log_file_path + " | wc -l"
    cmd_2 = "grep 'Error' " + log_file_path
    output_1 = subprocess.check_output(cmd_1, stderr=subprocess.STDOUT, shell=True)
    output_2 = ''
    try:
        output_2 = subprocess.check_output(cmd_2, stderr=subprocess.STDOUT, shell=True)
    except Exception as e:
        pass
    number_of_error_lines = int(output_1[:-1])
    if number_of_error_lines != 0:
        return True, output_2
    else:
        return False, ''


def read_reco_files(rc_reco_path, non_rc_reco_path, path_recos_for_specialty_based_users, sepr_recos):
    '''This function will read the reco files and return pandas dataframe.

    Keyword arguments:
    rc_reco_path -- path of rc recommendations(string).
    non_rc_reco_path -- path of non rc recommendations(string).
    path_recos_for_specialty_based_users -- path of specialty recommendations(string).
    sepr_recos -- seperator for reco files.

    Output:
    rc_reco_df -- pandas dataframe for rc recomendations.
    non_rc_reco_df -- pandas dataframe for non rc recommendations.
    speciality_reco_df -- pandas dataframe for non rc specialty recommendations.
    '''

    # rc_reco_df = pd.read_csv(rc_reco_path,sep = sepr_recos)
    # non_rc_reco_df = pd.read_csv(non_rc_reco_path,sep = sepr_recos)
    # speciality_reco_df = pd.read_csv(path_recos_for_specialty_based_users,sep = sepr_recos)

    rc_reco_df = load_file_and_select_columns(rc_reco_path, sepr=sepr_recos)
    non_rc_reco_df = load_file_and_select_columns(non_rc_reco_path, sepr=sepr_recos)
    speciality_reco_df = load_file_and_select_columns(path_recos_for_specialty_based_users, sepr=sepr_recos)
    return rc_reco_df, non_rc_reco_df, speciality_reco_df


def load_files_score_matrix_validation(spark, path_score_matrix, path_content_score_matrix, path_approved_links,
                                       cols_approved_links \
                                       , path_metadata, cols_metadata, sepr, approved_bool, article_id, sepr_recos):
    '''This function will load required files for score matrix validation.

    spark -- spark object.
    path_score_matrix -- path of graph score matrix(string).
    path_content_score_matrix -- path of content score matrix(string).
    path_approved_links -- path of rc approved articles file(string).
    cols_approved_links -- list of significant columns in rc approved articles file.
    path_metadata -- path of non rc metadata in store_inputs(string).
    cols_metadata -- list of significant columns in non rc metadata file in store_inputs(string).
    sepr -- separator(string).
    approved_bool -- column in metadata specifying if the article is approved or not(string). 
    article_id -- column for article's id(string).
    sepr_recos -- seperator for recos files(string).

    Output -- graph score matrix, content similarity score matrix and list of all approved articles
    '''
    g_score_matrix = load_file_and_select_columns(path_score_matrix, sepr_recos)
    c_score_matrix = load_file_and_select_columns(path_content_score_matrix, sepr)
    dh_metadata = load_metadata(
        spark, path_metadata, cols_metadata, sepr)
    df_approved_links = load_file_and_select_columns(
        path_approved_links, sepr, cols_approved_links, spark)
    # generate non rc approved article list
    non_rc_approved_articles = find_approved_article_list(
        dh_metadata, approved_bool, article_id)
    rc_approved_articles = make_unique_col_list(df_approved_links, article_id)
    all_approved_articles = list(non_rc_approved_articles)
    all_approved_articles.extend(list(rc_approved_articles))
    return g_score_matrix, c_score_matrix, all_approved_articles


def check_if_score_matrices_are_valid(spark, path_score_matrix, path_content_score_matrix, path_approved_links,
                                      cols_approved_links \
                                      , path_metadata, cols_metadata, sepr, approved_bool, article_id, sepr_recos,
                                      ss_score_col):
    '''This wrapper function will validate the score matrices.

    Keyword arguments:
    spark -- spark object
    path_score_matrix -- path of graph score matrix
    path_content_score_matrix -- path of content similarity score matrix
    path_approved_links --  path of resource center approved links file.
    cols_approved_links -- required cols in approved_links
    path_metadata -- path of non rc metadata file.
    sepr -- seperator
    approved_bool -- a boolean value specifying whether article is approved or not.
    article_id -- column name ofr article's id
    sepr_recos -- seperator for recos
    ss_score_col -- column name for score columns
    '''
    # load all required files
    g_score_matrix, c_score_matrix, all_approved_articles = load_files_score_matrix_validation(
        spark, path_score_matrix, \
        path_content_score_matrix, path_approved_links, cols_approved_links \
        , path_metadata, cols_metadata, sepr, approved_bool, article_id, sepr_recos)

    expected_sum_list = [0, 0, 0]
    if (g_score_matrix.isnull().sum().tolist() != expected_sum_list):
        send_mail("graph score had null values")
    # load content similarity score matrix
    if (c_score_matrix.isnull().sum().tolist() != expected_sum_list):
        send_mail("content score had null values")
    # check if id_y contains only approved articles
    if (g_score_matrix[~g_score_matrix[id_y].isin(all_approved_articles)].shape[0] != 0):
        send_mail("graph score matrix had non approved articles in id_y")
    if (c_score_matrix[~c_score_matrix[id_y].isin(all_approved_articles)].shape[0] != 0):
        send_mail("content score matrix had non approved articles in id_y")
    # check if content similarity score is in the range(-1,0.9)
    max_cs_score = 0.9
    check_content_similarity_score_range(c_score_matrix, path_content_score_matrix, ss_score_col, max_cs_score)


def check_content_similarity_score_range(c_score_matrix, path_content_score_matrix, ss_score_col, max_cs_score):
    '''This function will check if content similarity score is in range (-1,0.9)'''
    if ((c_score_matrix[c_score_matrix[ss_score_col] > max_cs_score].shape[0] != 0) | \
            (c_score_matrix[c_score_matrix[ss_score_col] < -1].shape[0] != 0)):
        send_mail('content similarity score beyond the range')


def check_reco_types_in_reco_file(rc_reco, non_rc_reco, specialty_recos, expected_reco_flag_in_rc_reco,
                                  expected_reco_flag_in_non_rc_reco, expected_reco_flag_in_specialty_reco):
    '''This function will check if necessary reco flags were present in reco file'''
    passed_rc = True
    passed_non_rc = True
    passed_specialty = True
    expected_reco_flag_in_rc_reco.sort()
    expected_reco_flag_in_non_rc_reco.sort()
    expected_reco_flag_in_specialty_reco.sort()
    rc_reco_types = make_unique_col_list(rc_reco, 'reco_flag')
    non_rc_reco_types = make_unique_col_list(non_rc_reco, 'reco_flag')
    specialty_reco_types = make_unique_col_list(specialty_recos, 'reco_flag')
    rc_reco_types.sort()
    non_rc_reco_types.sort()
    specialty_reco_types.sort()
    if (rc_reco_types != expected_reco_flag_in_rc_reco):
        send_mail('rc reco types in reco file is not as expected')
        passed_rc = False
    if (non_rc_reco_types != expected_reco_flag_in_non_rc_reco):
        send_mail('non-rc reco types in reco file is not as expected')
        passed_non_rc = False
    if (specialty_reco_types != expected_reco_flag_in_specialty_reco):
        send_mail('specialty reco types in reco file is not as expected')
        passed_specialty = False
    return passed_rc, passed_non_rc, passed_specialty


def check_if_lookback_recos_are_removed(non_rc_reco, rc_reco, non_rc_lookback, rc_lookback, user_id, article_id):
    '''This wrapper function will check if those recommendations are removed or not which 
        has been seen by the user in past 2 months.

        Keyword arguments:
    '''

    passed = True
    # check for non rc recos
    df_1 = rc_reco.merge(rc_lookback, on=[user_id, article_id], how='inner')
    df_2 = non_rc_reco.merge(non_rc_lookback, on=[user_id, article_id], how='inner')

    if (df_1.shape[0] != 0):
        passed = passed & False
        send_mail("rc lookback articles not removed")
    # check for rc recos
    if (df_2.shape[0] != 0):
        passed = passed & False
        send_mail("non rc lookback articles not removed")
    return passed


def check_if_rc_restriction_recos_are_removed(spark, rc_recos_df, df_rc_restriction_lookback):
    '''This wrapper function will check if no user if sent a recommendation if it has been
        send 5 times.
    '''

    # convert the dataframes to pandas
    df_1 = rc_recos_df.join(df_rc_restriction_lookback, on=[user_id, article_id], how='inner')
    # filter and check
    if (df_1.count() != 0):
        send_mail("Rc recommendations send more than 5 times not filtered from rc recos")
    else:
        print('rc restriction recos removed and checked')


def check_if_userid_in_reco_are_present_in_demo_file(rc_reco, non_rc_reco, specialty_recos, demo, rc_targets):
    '''This wrapper function will check if the user present in reco file is also there in demo/rc_targets file or not'''

    # find list of users in required dataframes
    passed_rc = True
    passed_non_rc = True
    passed_specialty = True
    list_rc_target_user_id = make_unique_col_list(rc_targets, user_id)
    list_demo_user_id = make_unique_col_list(demo, user_id)
    list_user_rc_reco = make_unique_col_list(rc_reco, user_id)
    list_user_non_rc_reco = make_unique_col_list(non_rc_reco, user_id)
    list_user_specialty_reco = make_unique_col_list(specialty_recos, user_id)
    # check and send mail
    if (len(set(list_user_rc_reco) - set(list_rc_target_user_id)) != 0):
        send_mail("rc reco had users not present in rc targets file")
        passed_rc = False
    if (len(set(list_user_non_rc_reco) - set(list_demo_user_id)) != 0):
        send_mail("non rc reco had users not present in demo file")
        passed_non_rc = False
    if (len(set(list_user_specialty_reco) - set(list_demo_user_id)) != 0):
        send_mail("specialty reco had users not present in demo file")
        passed_specialty = False
    return passed_rc, passed_non_rc, passed_specialty


def check_if_reco_columns_as_expected(rc_reco, non_rc_reco, specialty_recos, expected_reco_col_list):
    '''This wrapper function will check if the reco files had expected columns'''
    passed_rc = True
    passed_non_rc = True
    passed_specialty = True
    # reco columns
    rc_reco_columns = rc_reco.columns.tolist()
    non_rc_reco_columns = non_rc_reco.columns.tolist()
    specialty_reco_columns = specialty_recos.columns.tolist()
    # sort the list
    rc_reco_columns.sort()
    non_rc_reco_columns.sort()
    specialty_reco_columns.sort()
    # check
    if (rc_reco.columns.tolist() != expected_reco_col_list):
        send_mail('rc recommendations did not have expected columns')
        passed_rc = False
    if (non_rc_reco.columns.tolist() != expected_reco_col_list):
        send_mail('non rc recommendations did not have expected columns')
        passed_non_rc = False
    if (specialty_recos.columns.tolist() != expected_reco_col_list):
        send_mail('specialty recommendations did not have expected columns')
        passed_specialty = False
    return passed_rc, passed_non_rc, passed_specialty


def check_if_article_ids_are_from_metadata_per_file(reco, approved_metadata, article_id):
    '''This function will check if article ids are as from metadata for each reco file'''
    return (reco[~reco[article_id].isin(approved_metadata[article_id])].shape[0] == 0)


def check_if_article_ids_are_from_metadata(df_rc_appr, non_rc_meta_approved, rc_reco, non_rc_reco, specialty_recos,
                                           article_id):
    '''This wrapper function checks if article ids in reco file are approved articles'''
    # load metadata and filter for approved articles
    passed_rc = True
    passed_non_rc = True
    passed_specialty = True
    # apply checks
    if (check_if_article_ids_are_from_metadata_per_file(rc_reco, df_rc_appr, article_id) != True):
        send_mail("rc reco article id not in rc approved articles")
        passed_rc = False
    if (check_if_article_ids_are_from_metadata_per_file(non_rc_reco, non_rc_meta_approved, article_id) != True):
        send_mail("non rc reco article id not in non rc approved articles")
        passed_non_rc = False
    if (check_if_article_ids_are_from_metadata_per_file(specialty_recos, non_rc_meta_approved, article_id) != True):
        send_mail("specialty reco article id not in non rc approved articles")
        passed_specialty = False
    return passed_rc, passed_non_rc, passed_specialty


def check_if_all_users_are_sent_reco(rc_reco_df, non_rc_reco_df, speciality_reco_df, path_rc_targets, path_demo):
    '''This wrapper function will verify if all users are sent reco and send the count of users not sent reco if needed'''
    # check all  users sent rc reco
    try:
        # rc_target = pd.read_csv(path_rc_targets,sep=sepr)
        rc_target = load_file_and_select_columns(path_rc_targets, sepr=sepr)
        not_sent = find_user_not_sent_reco(rc_reco_df, rc_target)
        if not_sent > 0:
            send_mail(str(not_sent) + ' rc target user not have reco generated for them')
    except:
        send_mail('check failed:rc target user presence in reco')
    # check almost all user sent non rc reco
    try:
        # non_rc_target = pd.read_csv(path_demo,sep = sepr)
        non_rc_target = load_file_and_select_columns(path_demo, sepr=sepr)
        df_non_rc_reco_new = non_rc_reco_df.append(speciality_reco_df)
        not_sent = find_user_not_sent_reco(df_non_rc_reco_new, non_rc_target)
        if not_sent > 0:
            send_mail(str(not_sent) + ' non rc target user not have reco generated for them')
    except:
        send_mail(' check failed:non rc target user presence in reco')


def check_number_of_recos_per_user(non_rc_reco, no_of_recommendations, user_id, article_id):
    '''This function will check if the number of recommendations sent per user in not less than required values.'''
    count_recos_df = non_rc_reco.groupby(user_id).TBID.count().reset_index()
    count_recos_df = count_recos_df.rename(columns={'TBID': 'count_recos'})
    # count number of users who were sent fewer recos
    num_users = count_recos_df[count_recos_df['count_recos'] < no_of_recommendations][user_id].nunique()
    if (num_users != 0):
        send_mail(str(num_users) + ' non rc users not have required number of recos generated for them')


def check_reco_type_and_user_type_are_consistent(rc_reco, non_rc_reco, path_graph_reco_user_list \
                                                 , path_graph_random_user_list, path_content_reco_user_list):
    '''This function will check if reco types are consistent with the type of user(graph,content or random).

    Keyword arguments:
    rc_reco -- pandas dataframe for rc recommendations.
    non_rc_reco -- pandas dataframe for non rc recommendations.
    path_graph_reco_user_list -- path of list of users for graph Users.
    path_content_reco_user_list -- path of list of users for content recommendations.
    '''

    graph_user_list = load_list_from_pickle(path_graph_reco_user_list)
    random_user_list = load_list_from_pickle(path_graph_random_user_list)
    content_user_list = load_list_from_pickle(path_content_reco_user_list)
    # get rc recos users of particular recotypes
    rc_graph_reco_user = rc_reco[rc_reco['reco_flag'] == graph_based][user_id].unique().tolist()
    rc_content_reco_user = rc_reco[rc_reco['reco_flag'] == content_based][user_id].unique().tolist()
    rc_random_reco_user = rc_reco[rc_reco['reco_flag'] == random_based][user_id].unique().tolist()
    # get non rc recos of particular recotypes
    non_rc_graph_reco_user = non_rc_reco[non_rc_reco['reco_flag'] == graph_based][user_id].unique().tolist()
    non_rc_content_reco_user = non_rc_reco[non_rc_reco['reco_flag'] == content_based][user_id].unique().tolist()
    non_rc_random_reco_user = non_rc_reco[non_rc_reco['reco_flag'] == random_based][user_id].unique().tolist()
    # check
    if len(set(rc_graph_reco_user) - set(graph_user_list)) != 0:
        send_mail('rc graph reco was generated for non graph users')
    if len(set(rc_content_reco_user) - set(content_user_list)) != 0:
        send_mail('rc content reco was generated for non content users')
    if len(set(rc_random_reco_user) - set(random_user_list)) != 0:
        send_mail('rc random reco was generated for non random users')
    if len(set(non_rc_graph_reco_user) - set(graph_user_list)) != 0:
        send_mail('non rc graph reco was generated for non graph users')
    if len(set(non_rc_content_reco_user) - set(content_user_list)) != 0:
        send_mail('non rc content reco was generated for non content users')


def check_number_of_reco_type_per_user_is_one(rc_reco, non_rc_reco, speciality_reco_df):
    '''This function will check if a user is sent reco with just 1 reco type or not'''
    if rc_reco.groupby(user_id)['reco_flag'].nunique().max() != 1:
        send_mail('rc recommendation has user who are having reco with more than 1 reco type')
    if non_rc_reco.groupby(user_id)['reco_flag'].nunique().max() != 1:
        send_mail('rc recommendation has user who are having reco with more than 1 reco type')
    if speciality_reco_df.groupby(user_id)['reco_flag'].nunique().max() != 1:
        send_mail('rc recommendation has user who are having reco with more than 1 reco type')
