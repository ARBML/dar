import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ANS_claim(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'claim_s':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0, 1])}))

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
		url = ['https://raw.githubusercontent.com/latynt/ans/master/data/claim/dev.csv', 'https://raw.githubusercontent.com/latynt/ans/master/data/claim/test.csv', 'https://raw.githubusercontent.com/latynt/ans/master/data/claim/train.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': {'inputs':[downloaded_files[0]]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': {'inputs':[downloaded_files[1]]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':[downloaded_files[2]]} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 2:
				continue
			df.columns = ['claim_s', 'fake_flag']
			for _, record in df.iterrows():
				yield str(_id), {'claim_s':record['claim_s'],'label':str(record['fake_flag'])}
				_id += 1 

