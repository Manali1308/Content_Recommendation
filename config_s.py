#!/home/ubuntu/anaconda/bin/ipython

# bxu updates
# import os
# os.chdir('/mnt01/pragalbh/tmp/bak3/graph_recos/codes_graph/reco_model_tmp')

# Test bak5
# delta_today = 12
# Production
delta_today = 0
# Test bak5
# main_folder='/mnt01/pragalbh/tmp/bak5/'
# Production
main_folder = '/mnt01/pragalbh/'

s3_folder = 'test2/'
s3_folder_s = 'test3/'

# eh-prodata-mpt/Connect/20181206
# eh-prodata-datasci/projects/content_recom/outgoing/connect/20181206
# S3://eh-prodata-datasci/projects/content_recom/outgoing/with_rc/test3/

bucket_name = 'eh-prodata-datasci'
# bxu update -added
mpt_bucket_name = 'eh-prodata-mpt'
# mpt_bucket_name = 'eh-prodata-datasci'


non_rc_reco_path_on_s3 = '/projects/content_recom/outgoing/with_rc/' + s3_folder
# bxu updates - added - commented due to S3 folder change
# non_rc_reco_path_on_s3_s = '/projects/content_recom/outgoing/with_rc/'+s3_folder_s
non_rc_reco_path_on_s3_s = '/projects/content_recom/outgoing/connect/'

# bxu updates - added
# non_rc_reco_path_on_s3_sum = '/projects/content_recom/outgoing/with_rc/'+s3_folder_s
non_rc_reco_path_on_s3_sum = '/projects/content_recom/outgoing/connect/'

# bxu updates - added - commented due to change
# non_rc_reco_path_on_s3_tsv = '/projects/content_recom/outgoing/with_rc/'+s3_folder_s
# bxu updates - added
non_rc_reco_path_on_s3_tsv = '/Connect/'

log_file_name = 'graph_recos/nohup_daily_log_12_12.out'

from pyspark.sql.types import *

path_dailyfeeds_on_s3 = '/ContentRecommendation/outgoing/'
path_dailyfeeds_on_instance = main_folder + 'graph_recos/store_dailyfeeds/'
start_date = 20180228
n_days_to_sync = 15
sepr = '|'

path_list_activity_dates = main_folder + 'graph_recos/store_inputs/list_activity_dates.p'
path_activity_dailyfeeds = main_folder + 'graph_recos/store_dailyfeeds/mpt_activity.txt'
path_activity = main_folder + 'graph_recos/store_inputs/mpt_activity.txt'
name_activity_on_s3 = '/mpt_activity.tar.gz'
path_activity_zip_on_instance = main_folder + 'graph_recos/store_dailyfeeds/mpt_activity.tar.gz'
# name_activity_on_s3 = '/mpt_activity.txt'

path_list_metadata_dates = main_folder + 'graph_recos/store_inputs/list_metadata_dates.p'
name_metadata_on_s3 = '/mpt_content_metadata.tar.gz'
path_metadata_zip_on_instance = main_folder + 'graph_recos/store_dailyfeeds/mpt_content_metadata.tar.gz'
# name_metadata_on_s3 = '/mpt_content_metadata.txt'

path_list_demo_dates = main_folder + 'graph_recos/store_inputs/list_demo_dates.p'
name_demo_on_s3 = '/mpt_demo.tar.gz'
path_demo_zip_on_instance = main_folder + 'graph_recos/store_dailyfeeds/mpt_demo.tar.gz'
# name_demo_on_s3 = '/mpt_demo.txt'

path_list_rc_metadata_dates = main_folder + 'graph_recos/store_inputs/list_rc_metadata_dates.p'
name_rc_metadata_on_s3 = '/mpt_ResourceCenter_Metadata.tar.gz'
path_rc_metadata_zip_on_instance = main_folder + 'graph_recos/store_dailyfeeds/mpt_ResourceCenter_Metadata.tar.gz'
# name_rc_metadata_on_s3 = '/mpt_ResourceCenter_Metadata.txt'

path_list_rc_approved_links_dates = main_folder + 'graph_recos/store_inputs/list_rc_approved_links_dates.p'
name_rc_approved_links_on_s3 = '/mpt_approvedlinks.tar.gz'
path_rc_approved_links_zip_on_instance = main_folder + 'graph_recos/store_dailyfeeds/mpt_approvedlinks.tar.gz'
# name_rc_approved_links_on_s3 = '/mpt_approvedlinks.txt'

path_list_rc_targets_dates = main_folder + 'graph_recos/store_inputs/list_rc_targets_dates.p'
name_rc_targets_on_s3 = '/mpt_ResourceCenter_Targets.tar.gz'
path_rc_targets_zip_on_instance = main_folder + 'graph_recos/store_dailyfeeds/mpt_ResourceCenter_Targets.tar.gz'
# name_rc_targets_on_s3 = '/mpt_ResourceCenter_Targets.txt'

path_list_feedback_date = main_folder + 'graph_recos/store_inputs/list_feedback_dates.p'
name_feedback_on_s3 = '/mpt_DisplayedOnsiteFeedback.tar.gz'
path_feedback_zip_on_instance = main_folder + 'graph_recos/store_dailyfeeds/mpt_DisplayedOnsiteFeedback.tar.gz'
# for data processor

path_metadata_dailyfeeds = main_folder + 'graph_recos/store_dailyfeeds/mpt_content_metadata.txt'
path_demo_dailyfeeds = main_folder + 'graph_recos/store_dailyfeeds/mpt_demo.txt'
path_rc_targets_dailyfeeds = main_folder + 'graph_recos/store_dailyfeeds/mpt_ResourceCenter_Targets.txt'
path_approved_links_dailyfeeds = main_folder + 'graph_recos/store_dailyfeeds/mpt_approvedlinks.txt'
path_rc_metadata_dailyfeeds = main_folder + 'graph_recos/store_dailyfeeds/mpt_ResourceCenter_Metadata.txt'
path_feedback_dailyfeeds = main_folder + 'graph_recos/store_dailyfeeds/mpt_DisplayedOnsiteFeedback.txt'

article_id = 'TBID'
non_rc_meta_title = 'Title'
rc_meta_title = 'Title'
non_rc_cattopicid = 'cattopicid'
article_category = 'TopCategory'
approved_bool = 'Approved_Content'
text_column_name = 'Review'
user_id = 'MasterUserID'
user_specialty = 'Specialty'
advertiser_id = 'AdvertiserID'
rc_content_column_name = 'Content'
source = 'Source'
activity_type = 'Activitytype'
rc_flag = 'isResourceCenter'
timestamp = 'Timestamp'
# feedback
sepr_feedback = '|'
start_date_feedback = 20180617
reco_flag_feedback = 'Reco_Flag'
eventdttm_feedback = 'EventDTTM'
is_click_feedback = 'isClick'
timestamp_format = 'MM/dd/yyyy HH:mm:ss'

cols_metadata = [article_id, article_category, approved_bool, text_column_name, non_rc_meta_title, non_rc_cattopicid]
cols_demo = [user_id, user_specialty]
# bxu updates - added
cols_demo2 = [user_id, 'AccountID', user_specialty]

cols_rc_targets = [user_id, advertiser_id]
cols_approved_links = [article_id, advertiser_id]
cols_rc_metadata = [article_id, rc_content_column_name, rc_meta_title]
cols_activity = [article_id, user_id,
                 activity_type, rc_flag, timestamp]
cols_lookback_file = [user_id, article_id]
cols_feedback = [user_id, article_id, reco_flag_feedback, eventdttm_feedback, is_click_feedback]

path_metadata = main_folder + 'graph_recos/store_inputs/mpt_content_metadata.txt'
path_demo = main_folder + 'graph_recos/store_inputs/mpt_demo.txt'
path_rc_targets = main_folder + 'graph_recos/store_inputs/mpt_ResourceCenter_Targets.txt'
path_approved_links = main_folder + 'graph_recos/store_inputs/mpt_approvedlinks.txt'
path_rc_metadata = main_folder + 'graph_recos/store_inputs/mpt_ResourceCenter_Metadata.txt'
path_activity = main_folder + 'graph_recos/store_inputs/mpt_activity.txt'
path_feedback = main_folder + 'graph_recos/store_inputs/mpt_DisplayedOnsiteFeedback.txt'

path_rc_lookback_file = main_folder + 'graph_recos/store_inputs/rc_lookback_file.txt'
path_non_rc_lookback_file = main_folder + 'graph_recos/store_inputs/non_rc_lookback_file.txt'

# graph config
cols_activity_graph = [timestamp, user_id, article_id]
days_to_consider = 90
# bxu updates
days_to_consider2 = 14
days_to_consider3 = 7

max_num_articles = 10000
gamma = 0.9
cutoff_length = 3
id_x = 'tbid_x'
id_y = 'tbid_y'
affinity_score = 'score_pair'
path_score_matrix = main_folder + 'graph_recos/store_intermediate/score_matrix.csv'
sepr_graph_matrix = ','

# headline sentiment and cattopic config
col_headline_sentiment = 'compound'
col_sentiment_tag = 'tag'
cattopicid_x = 'cattopicid_x'
cattopicid_y = 'cattopicid_y'
col_cattopic_score = 'cat_topic_score'
path_cattopic_score = main_folder + 'graph_recos/store_intermediate/cattopic_score.txt'
path_sentiment_score = main_folder + 'graph_recos/store_intermediate/sentiment_score.txt'

# content similarity config
path_preprocessed_article_list = main_folder + 'graph_recos/store_intermediate/preprocessed_article_list.p'
path_preprocessed_files = main_folder + 'graph_recos/store_intermediate/preprocessed_files.csv'
path_word2vec_model = main_folder + 'graph_recos/store_intermediate/word2vec_model'
path_content_score_matrix = main_folder + 'graph_recos/store_intermediate/content_score_matrix.csv'
word2vec_params = {'iter': 10, 'size': 100, 'window': 5,
                   'min_count': 1, 'workers': 8, 'seed': 1234}
index_column_name = article_id
path_relevant_article_ids = main_folder + 'graph_recos/store_intermediate/relevant_article_list.p'

# reco generator
rank_col = 'rank'
temp_rc_recos_path = main_folder + 'graph_recos/store_outputs/temp_rc_recos_path'
# bxu updates - added
temp_rc_recos_path_s = main_folder + 'graph_recos/store_outputs/temp_rc_recos_path_s'
temp_non_rc_recos_path = main_folder + 'graph_recos/store_outputs/temp_non_rc_recos_path'
# bxu updates - added
temp_non_rc_recos_path_s = main_folder + 'graph_recos/store_outputs/temp_non_rc_recos_path_s'
# bxu updates - added
temp_path_non_rc_recos_path_s = main_folder + 'graph_recos/store_outputs/temp_non_rc_recos_path_s/non_specialty.csv'
# bxu updates - added
temp_non_rc_recos_path_sc = main_folder + 'graph_recos/store_outputs/temp_non_rc_recos_path_sc'

sepr_recos = ','
rc_reco_path = main_folder + 'graph_recos/store_outputs/rc_recommendations.csv'
# bxu updates - added
rc_reco_path_s = main_folder + 'graph_recos/store_outputs/rc_recommendations_s.csv'
non_rc_reco_path = main_folder + 'graph_recos/store_outputs/non_rc_recommendations.csv'
# bxu updates - added
non_rc_reco_path_s = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_s.csv'
# bxu updates - added
non_rc_reco_path_sc = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_sc.csv'
# bxu updates - added
non_rc_reco_path_tsv = main_folder + 'graph_recos/store_outputs/mpt_recommendations.tsv'
# bxu updates - added
temp_non_rc_recos_path_tsv = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_tsv'
# bxu updates - added
sepr_tsv = "\t"
# bxu updates - added
non_rc_recos_sum_path = main_folder + 'graph_recos/store_outputs/specialty_recos_summary'
# bxu updates - added
non_rc_recos_path_sum = main_folder + 'graph_recos/store_outputs/specialty_recos_summary.csv'

non_rc_max_reco = 300

# reommendation flag
content_based = 'c'
graph_based = 'g'
random_based = 'r'
specialty_based = 's'

# specialty recos config
no_of_recommendations = 30
temp_path_for_specialty_reco = main_folder + 'graph_recos/store_outputs/temp_specialty_recos_path'
# bxu updates - added
# temp_path_for_specialty_reco_s = main_folder+'graph_recos/store_outputs/temp_specialty_recos_path_s'
# bxu updates - added
# temp_path_for_specialty_reco_as = main_folder+'graph_recos/store_outputs/temp_specialty_recos_path_as'
path_recos_for_specialty_based_users = main_folder + 'graph_recos/store_outputs/specialty_recos.csv'
# bxu updates - added
path_recos_for_specialty_based_users_s = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_s.csv'
# bxu updates - added
path_recos_for_specialty_based_users_s_bak = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_s_bak.csv'

specialty_user_list_path = specialty_user_list_path = main_folder + 'graph_recos/store_intermediate/specialty_user_list.p'
specialty_random_user_list_path = main_folder + 'graph_recos/store_intermediate/specialty_random_user_list.p'
temp_path_for_random_recos = main_folder + 'graph_recos/store_outputs/temp_specialty_recos_path/random.csv'

# bxu updates
temp_path_non_specialty_recos = main_folder + 'graph_recos/store_outputs/temp_specialty_recos_path/non_specialty.csv'

# post processing
# post processing
path_cc_restriction_lookback = main_folder + 'graph_recos/store_intermediate/cc_restriction_lookback.csv'
path_rc_reco_dump_file = main_folder + 'graph_recos/store_intermediate/rc_reco_dump_file.csv'
temp_path_rc_reco_dump_file = main_folder + 'graph_recos/store_intermediate/temp_path_rc_reco_dump_file'
temp_path_cc_restriction_lookback = main_folder + 'graph_recos/store_intermediate/temp_path_cc_restriction_lookback'
rc_reco_dump_elsewhere_path = main_folder + 'graph_recos/store_intermediate/temp_rc_reco_dumper/'

# path_list_graph_content_missed_users = '/mnt01/pragalbh/graph_recos/store_intermediate/graph_content_missed_users.p'
path_list_graph_content_missed_users = main_folder + '/graph_recos/store_intermediate/graph_content_missed_users.p'

n_cc_reco = 3
n_cc_restriction = 5

sepr_restriction_dump = ','
# bxu updates - commented
n_non_cc_reco = 20
# bxu updates - added
n_non_cc_reco10 = 10
n_non_cc_reco20 = 20

# reco uploader
non_rc_reco_path_renamed = main_folder + 'graph_recos/store_outputs/non_rc_recommendations'
# bxu updates - added
non_rc_reco_path_renamed_s = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_s'
# bxu updates - added
non_rc_reco_path_renamed_sum = main_folder + 'graph_recos/store_outputs/specialty_recos_summary'
# bxu updates - added
non_rc_reco_path_renamed_sc = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_sc'
# bxu updates - added
non_rc_reco_path_renamed_tsv = main_folder + 'graph_recos/store_outputs/mpt_recommendations'

rc_reco_path_renamed = main_folder + 'graph_recos/store_outputs/rc_recommendations'
# bxu updates - added
rc_reco_path_renamed_s = main_folder + 'graph_recos/store_outputs/rc_recommendations_s'
rc_reco_path_on_s3 = '/projects/content_recom/outgoing/with_rc/' + s3_folder
# bxu updates - added
rc_reco_path_on_s3_s = '/projects/content_recom/outgoing/with_rc/' + s3_folder_s

specialty_reco_path_renamed = main_folder + 'graph_recos/store_outputs/specialty_recos'
specialty_reco_path_on_s3 = '/projects/content_recom/outgoing/with_rc/' + s3_folder

specialty_reco_path = main_folder + 'graph_recos/store_outputs/specialty_recos.csv'
# bxu updates - added
specialty_reco_path_s = main_folder + 'graph_recos/store_outputs/non_rc_recommendations_s.csv'

# USER LIST PICKLES
path_graph_reco_user_list = main_folder + 'graph_recos/store_intermediate/graph_reco_user_list.p'
path_graph_random_user_list = main_folder + 'graph_recos/store_intermediate/graph_random_user_list.p'
path_content_reco_user_list = main_folder + 'graph_recos/store_intermediate/content_reco_user_list.p'
# path_content_random_user_list = main_folder+'graph_recos/store_intermediate/content_random_user_list.p'
path_speciality_reco_user_list = main_folder + 'graph_recos/store_intermediate/speciality_reco_user_list.p'
# bxu updates - added
path_speciality_reco_user_list_all = main_folder + 'graph_recos/store_intermediate/speciality_reco_user_list_all.p'
# bxu updates - added
path_speciality_reco_user_list_s = main_folder + 'graph_recos/store_intermediate/speciality_reco_user_list_s.p'
# path_specialty_random_user_list = main_folder+'graph_recos/store_intermediate/specialty_random_user_list.p'
path_non_specialty_user_list = main_folder + 'graph_recos/store_intermediate/non_specialty_user_list.p'
# bxu updates - added
path_non_specialty_user_list_s = main_folder + 'graph_recos/store_intermediate/non_specialty_user_list_s.p'

pct_random = 50
random_cohort_span = 7
path_random_user_dates_list = main_folder + 'graph_recos/store_intermediate/random_user_dates.p'
path_graph_random_user_folder = main_folder + 'graph_recos/store_intermediate/random_user/'
path_graph_reco_user_folder = main_folder + 'graph_recos/store_intermediate/random_user/'

# schemas
reco_schema = StructType([
    StructField(user_id, LongType(), True),
    StructField(article_id, LongType(), True),
    StructField(rank_col, IntegerType(), True),
    StructField("reco_flag", StringType(), True)])

# bxu updates -added
reco_schema2 = StructType([
    StructField("MasterUserID", LongType(), True),
    StructField("site_property", StringType(), True),
    StructField("account_id", LongType(), True),
    StructField("ID_type", StringType(), True),
    StructField("ID", LongType(), True),  # i.e., Tbid
    StructField("rank", IntegerType(), True),
    StructField("reco_model", StringType(), True),
    StructField("model_version", StringType(), True),
    StructField("reason", StringType(), True)])  # i.e., Speicialty

# bxu updates -added
cols_non_rc_reco_sc = ["MasterUserID", "site_property", "account_id", "ID_type", "ID", "rank", "reco_model",
                       "model_version", "reason"]

restriction_schema = StructType([StructField(user_id, LongType(), True), StructField(
    article_id, LongType(), True)])

dump_schema = StructType([
    StructField(user_id, LongType(), True),
    StructField(article_id, LongType(), True),
    StructField('num_times', IntegerType(), True)])

# similarity score config
ss_rank_col = 'ss_rank'
ss_score_col = 'similarity_score'
top_n_ss = 200

temp_path_graph_table = main_folder + 'graph_recos/store_intermediate/temp_path_graph_table'

# alert system
path_reco_analysis = main_folder + 'graph_recos/store_intermediate/path_reco_analysis.txt'
# bxu updates - added
path_reco_analysis_s = main_folder + 'graph_recos/store_intermediate/path_reco_analysis_s.txt'
reco_analysis_file_name = 'path_reco_analysis.txt'

rc_reco_path_download_on_instance = main_folder + 'graph_recos/store_downloads/rc_recommendations.csv'
non_rc_reco_path_download_on_instance = main_folder + 'graph_recos/store_downloads/non_rc_recommendations.csv'
# bxu updates - commented
speciality_path_download_on_instance = main_folder + 'graph_recos/store_downloads/specialty_recos.csv'
# bxu updates - added
speciality_path_download_on_instance_s = main_folder + 'graph_recos/store_downloads/non_rc_recommendations_s.csv'

# bxu updates - commented
# log_file_path = main_folder+'graph_recos/nohup_daily_log10.out'
# bxu updates - added
log_file_path = main_folder + log_file_name

# eh_kf_mail_list = ['jtodd@everydayhealthinc.com','DRoberts@everydayhealthinc.com','gourav@knowledgefoundry.net','rishabh@knowledgefoundry.net','dhruvil@knowledgefoundry.net']
# kf_mail_list = ['gourav@knowledgefoundry.net','rishabh@knowledgefoundry.net','dhruvil@knowledgefoundry.net']
# cut_off_data_pinging_time = "20:15:00"

# eh_kf_mail_list = ['jtodd@everydayhealthinc.com','DRoberts@everydayhealthinc.com','gourav@knowledgefoundry.net','rishabh@knowledgefoundry.net','dhruvil@knowledgefoundry.net']
# kf_mail_list = ['gourav@knowledgefoundry.net','rishabh@knowledgefoundry.net','dhruvil@knowledgefoundry.net']
eh_kf_mail_list = ['bxu@everydayhealthgroup.com']
kf_mail_list = ['bxu@everydayhealthgroup.com']
# cut_off_data_pinging_time = "20:30:00"
cut_off_data_pinging_time = "10:00:00"

expected_reco_flag_in_rc_reco = [u'c', u'g', u'r', u's']
expected_reco_flag_in_non_rc_reco = [u'c', u'g', u'r']
expected_reco_flag_in_specialty_reco = [u's']
expected_reco_col_list = [user_id, article_id, 'rank', 'reco_flag']
