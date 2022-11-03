import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class RES1(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'restaurant_id':datasets.Value('string'),'user_id':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[1, -1])}))

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
		url = ['https://raw.githubusercontent.com/hadyelsahar/large-arabic-sentiment-analysis-resouces/master/datasets/RES1.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 4:
				continue
			df.columns = ['polarity', 'text', 'restaurant_id', 'user_id']
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'restaurant_id':record['restaurant_id'],'user_id':record['user_id'],'label':str(record['polarity'])}
				_id += 1 

