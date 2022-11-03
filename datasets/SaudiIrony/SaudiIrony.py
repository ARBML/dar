import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class SaudiIrony(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Tweet ID':datasets.Value('string'),'Tweets with Decoded emojis':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['تهكم', 'ليست تهكم'])}))

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
		url = ['https://raw.githubusercontent.com/iwan-rg/Saudi-Dialect-Irony-Dataset/main/SaudiIrony.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 3:
				continue
			df.columns = ['Tweet ID', 'Tweets with Decoded emojis', 'Final Annotation']
			for _, record in df.iterrows():
				yield str(_id), {'Tweet ID':record['Tweet ID'],'Tweets with Decoded emojis':record['Tweets with Decoded emojis'],'label':str(record['Final Annotation']).strip()}
				_id += 1 

