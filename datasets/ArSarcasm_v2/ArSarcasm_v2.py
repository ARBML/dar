import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ArSarcasm_v2(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet':datasets.Value('string'),'sentiment':datasets.Value('string'),'dialect':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[False, True])}))

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
		url = ['https://raw.githubusercontent.com/iabufarha/ArSarcasm-v2/main/ArSarcasm-v2/testing_data.csv', 'https://raw.githubusercontent.com/iabufarha/ArSarcasm-v2/main/ArSarcasm-v2/training_data.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': {'inputs':[downloaded_files[0]]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':[downloaded_files[1]]} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 4:
				continue
			df.columns = ['tweet', 'sarcasm', 'sentiment', 'dialect']
			for _, record in df.iterrows():
				yield str(_id), {'tweet':record['tweet'],'sentiment':record['sentiment'],'dialect':record['dialect'],'label':str(record['sarcasm'])}
				_id += 1 

