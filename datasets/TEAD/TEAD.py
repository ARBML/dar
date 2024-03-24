import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json
class TEAD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id_str':datasets.Value('string'),'text':datasets.Value('string'),'clean':datasets.Value('string'),'not_clean':datasets.Value('string'),'n_emoji':datasets.Value('string'),'n_hashtag':datasets.Value('string'),'n_mention':datasets.Value('string'),'n_url':datasets.Value('string'),'ln':datasets.Value('string'),'score':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['negative', 'positive'])}))

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
		url = ['https://github.com/HSMAabdellaoui/TEAD/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/TEAD-master/**.json')),} })]


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
			df.columns = ['id_str', 'text', 'clean', 'sentiment', 'not_clean', 'n_emoji', 'n_hashtag', 'n_mention', 'n_url', 'ln', 'score']
			for _, record in df.iterrows():
				yield str(_id), {'id_str':record['id_str'],'text':record['text'],'clean':record['clean'],'not_clean':record['not_clean'],'n_emoji':record['n_emoji'],'n_hashtag':record['n_hashtag'],'n_mention':record['n_mention'],'n_url':record['n_url'],'ln':record['ln'],'score':record['score'],'label':str(record['sentiment'])}
				_id += 1 

