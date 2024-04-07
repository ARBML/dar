import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json
class Egyptial_Gulf_Twitter_Dataset(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'created_at':datasets.Value('string'),'id':datasets.Value('string'),'id_str':datasets.Value('string'),'text':datasets.Value('string'),'source':datasets.Value('string'),'truncated':datasets.Value('string'),'in_reply_to_status_id':datasets.Value('string'),'in_reply_to_status_id_str':datasets.Value('string'),'in_reply_to_user_id':datasets.Value('string'),'in_reply_to_user_id_str':datasets.Value('string'),'in_reply_to_screen_name':datasets.Value('string'),'user':datasets.Value('string'),'geo':datasets.Value('string'),'coordinates':datasets.Value('string'),'place':datasets.Value('string'),'contributors':datasets.Value('string'),'retweeted_status':datasets.Value('string'),'is_quote_status':datasets.Value('string'),'quote_count':datasets.Value('string'),'reply_count':datasets.Value('string'),'retweet_count':datasets.Value('string'),'favorite_count':datasets.Value('string'),'entities':datasets.Value('string'),'favorited':datasets.Value('string'),'retweeted':datasets.Value('string'),'possibly_sensitive':datasets.Value('string'),'filter_level':datasets.Value('string'),'lang':datasets.Value('string'),'timestamp_ms':datasets.Value('string'),'display_text_range':datasets.Value('string'),'quoted_status_id':datasets.Value('string'),'quoted_status_id_str':datasets.Value('string'),'quoted_status':datasets.Value('string')}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'wav', 'mp3', 'jpg', 'png']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/telsahy/capstone-34/master/34.210.104.115/eg_twitter_raw/stream_أزاي.jsonl', 'https://raw.githubusercontent.com/telsahy/capstone-34/master/34.210.104.115/eg_twitter_raw/stream_ازيك.jsonl', 'https://raw.githubusercontent.com/telsahy/capstone-34/master/34.210.104.115/eg_twitter_raw/stream_السكه.jsonl', 'https://raw.githubusercontent.com/telsahy/capstone-34/master/34.210.104.115/eg_twitter_raw/stream_ضوافر.jsonl', 'https://raw.githubusercontent.com/telsahy/capstone-34/master/34.210.104.115/eg_twitter_raw/stream_كوباية.jsonl', 'https://raw.githubusercontent.com/telsahy/capstone-34/master/34.210.104.115/eg_twitter_raw/stream_مناخير.jsonl']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def read_json(self, filepath, json_key, lines = False):
		if json_key:
			data = json.load(open(filepath))
			df = pd.DataFrame(data[json_key]) 
		else:
			df = pd.read_json(filepath, lines=lines)
		return df

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_json(filepath, lines=True, json_key='')
			if len(df.columns) != 35:
				continue
			df = df[['created_at', 'id', 'id_str', 'text', 'source', 'truncated', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'retweeted_status', 'is_quote_status', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'entities', 'favorited', 'retweeted', 'possibly_sensitive', 'filter_level', 'lang', 'timestamp_ms', 'display_text_range', 'quoted_status_id', 'quoted_status_id_str', 'quoted_status']]
			for _, record in df.iterrows():
				yield str(_id), {'created_at':record['created_at'],'id':record['id'],'id_str':record['id_str'],'text':record['text'],'source':record['source'],'truncated':record['truncated'],'in_reply_to_status_id':record['in_reply_to_status_id'],'in_reply_to_status_id_str':record['in_reply_to_status_id_str'],'in_reply_to_user_id':record['in_reply_to_user_id'],'in_reply_to_user_id_str':record['in_reply_to_user_id_str'],'in_reply_to_screen_name':record['in_reply_to_screen_name'],'user':record['user'],'geo':record['geo'],'coordinates':record['coordinates'],'place':record['place'],'contributors':record['contributors'],'retweeted_status':record['retweeted_status'],'is_quote_status':record['is_quote_status'],'quote_count':record['quote_count'],'reply_count':record['reply_count'],'retweet_count':record['retweet_count'],'favorite_count':record['favorite_count'],'entities':record['entities'],'favorited':record['favorited'],'retweeted':record['retweeted'],'possibly_sensitive':record['possibly_sensitive'],'filter_level':record['filter_level'],'lang':record['lang'],'timestamp_ms':record['timestamp_ms'],'display_text_range':record['display_text_range'],'quoted_status_id':record['quoted_status_id'],'quoted_status_id_str':record['quoted_status_id_str'],'quoted_status':record['quoted_status']}
				_id += 1 

