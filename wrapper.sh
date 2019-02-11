#!/bin/bash
export PYTHONPATH=$PYTHONPATH:/opt/spark/spark-2.1.0-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip:/opt/spark/spark-2.1.0-bin-hadoop2.7/python/
PIFS=$IFS; IFS=$'\n'; for i in `jupyter notebook list  2> /dev/null | grep http`; do   j=`echo $i| sed 's/.*\:\([0-9]\+\)\/.*/\1/' `; k=`lsof -n -i4TCP:$j | grep -m 1 "jupyter"| awk -F ' ' '{printf $2}' `; kill -15 $k; done ; IFS=$PIFS
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/data_importer.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/reco_uploader.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/data_preprocessor.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/generate_lookback_file.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/graph_score_generator.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/text_corpus_cleaner.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/content_similarity_score_generator.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/cattopic_and_headline_sentiment.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/reco_user_list_creator.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/rc_graph_recos.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/rc_content_recos.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/rc_random_recos.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/rc_specialty_recos.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/non_rc_graph_recos.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/non_rc_content_recos.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/non_rc_random_recos.py
#/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/non_rc_specialty_recos.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/non_rc_specialty_recos_s0.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/postprocessing.py
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/reco_validator_and_uploader.py
#/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment/test_utils.py

echo "stepdone 0 - Ori Processes are done"
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/reco_uploader_s.py
echo "stepdone 1+ - reco_uploader_s.py"
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/reco_user_list_creator_s.py
echo "stepdone 9+ - reco_user_list_creator_s.py"
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/non_rc_specialty_recos_s.py
echo "stepdone 17+ - non_rc_specialty_recos_s.py"
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/postprocessing_s.py
echo "stepdone 18+ - postprocessing_s.py"
/home/ubuntu/anaconda/bin/ipython /mnt01/pragalbh/graph_recos/codes_graph/reco_model_cattopic_id_score_headline_sentiment_1/reco_validator_and_uploader_s.py
echo "stepdone 19+ - reco_validator_and_uploader_s.py"
