import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class AFND(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'title':datasets.Value('string'),'text':datasets.Value('string'),'published date':datasets.Value('string')}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff'] 
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/67mhx6hhzd-1.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(downloaded_files[0]+'/67mhx6hhzd-1/AFND/Dataset/**/scraped_articles.json')})]


	def read_json(self, filepath, json_key, lines = False):
		if json_key:
			data = json.load(open(filepath))
			df = pd.DataFrame(data[json_key]) 
		else:
			df = pd.read_json(filepath, lines=lines)
		return df
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = self.read_json(filepath, lines=False, json_key='articles')
			if len(df.columns) != 3:
				continue
			df.columns = ['title', 'text', 'published date']
			for _, record in df.iterrows():
				yield str(_id), {'title':record['title'],'text':record['text'],'published date':record['published date']}
				_id += 1 

