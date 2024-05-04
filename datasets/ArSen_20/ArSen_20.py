import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ArSen_20(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet id':datasets.Value('string'),'author id':datasets.Value('string'),'created_at':datasets.Value('string'),'lang':datasets.Value('string'),'like_count':datasets.Value('string'),'quote_count':datasets.Value('string'),'reply_count':datasets.Value('string'),'retweet_count':datasets.Value('string'),'tweet':datasets.Value('string'),'user_verified':datasets.Value('string'),'followers_count':datasets.Value('string'),'following_count':datasets.Value('string'),'tweet_count':datasets.Value('string'),'listed_count':datasets.Value('string'),'name':datasets.Value('string'),'username':datasets.Value('string'),'user_created_at':datasets.Value('string'),'description':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['negative', 'neutral', 'positive'])}))

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
		url = ['https://raw.githubusercontent.com/123fangyang/ArSen-20/main/data/ArSen-20_publish.csv']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r',', skiprows = 0, error_bad_lines = False, header = 0, encoding = 'utf-8')
			if len(df.columns) != 19:
				continue
			df = df[['tweet id', 'label', 'author id', 'created_at', 'lang', 'like_count', 'quote_count', 'reply_count', 'retweet_count', 'tweet', 'user_verified', 'followers_count', 'following_count', 'tweet_count', 'listed_count', 'name', 'username', 'user_created_at', 'description']]
			for _, record in df.iterrows():
				yield str(_id), {'tweet id':record['tweet id'],'author id':record['author id'],'created_at':record['created_at'],'lang':record['lang'],'like_count':record['like_count'],'quote_count':record['quote_count'],'reply_count':record['reply_count'],'retweet_count':record['retweet_count'],'tweet':record['tweet'],'user_verified':record['user_verified'],'followers_count':record['followers_count'],'following_count':record['following_count'],'tweet_count':record['tweet_count'],'listed_count':record['listed_count'],'name':record['name'],'username':record['username'],'user_created_at':record['user_created_at'],'description':record['description'],'label':str(record['label'])}
				_id += 1 

