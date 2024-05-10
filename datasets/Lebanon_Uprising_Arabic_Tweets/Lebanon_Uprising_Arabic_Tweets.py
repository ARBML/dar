import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json
class Lebanon_Uprising_Arabic_Tweets(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'screen_name':datasets.Value('string'),'created_at':datasets.Value('string'),'text':datasets.Value('string'),'retweet_count':datasets.Value('string'),'favorite_count':datasets.Value('string'),'in_reply_to_screen_name':datasets.Value('string'),'retweeted_status_screen_name':datasets.Value('string'),'user_description':datasets.Value('string'),'source':datasets.Value('string'),'lang':datasets.Value('string'),'id':datasets.Value('string')}))

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
		url = [os.path.abspath(os.path.expanduser(dl_manager.manual_dir))]
		downloaded_files = url
		self.extract_all(downloaded_files[0])
		files = self.get_all_files(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':files} })]


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
			df = self.read_json(filepath, lines=False, json_key='')
			if len(df.columns) != 11:
				continue
			df = df[['screen_name', 'created_at', 'text', 'retweet_count', 'favorite_count', 'in_reply_to_screen_name', 'retweeted_status_screen_name', 'user_description', 'source', 'lang', 'id']]
			for _, record in df.iterrows():
				yield str(_id), {'screen_name':record['screen_name'],'created_at':record['created_at'],'text':record['text'],'retweet_count':record['retweet_count'],'favorite_count':record['favorite_count'],'in_reply_to_screen_name':record['in_reply_to_screen_name'],'retweeted_status_screen_name':record['retweeted_status_screen_name'],'user_description':record['user_description'],'source':record['source'],'lang':record['lang'],'id':record['id']}
				_id += 1 

