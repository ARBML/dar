import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ElecMorocco(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'index':datasets.Value('string'),'comment_message':datasets.Value('string'),'comment_published':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['N', 'P'])}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff', 'wav', 'mp3']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/sentiprojects/ElecMorocco2016/master/ElecMorocco2016.xlsx']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 4:
				continue
			df.columns = ['index', 'comment_message', 'sentiment', 'comment_published']
			for _, record in df.iterrows():
				yield str(_id), {'index':record['index'],'comment_message':record['comment_message'],'comment_published':record['comment_published'],'label':str(record['sentiment'])}
				_id += 1 

