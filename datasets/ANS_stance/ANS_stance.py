import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ANS_stance(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'s1':datasets.Value('string'),'s2':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['disagree', 'agree', 'other'])}))

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
		url = ['https://raw.githubusercontent.com/latynt/ans/master/data/stance/dev.csv', 'https://raw.githubusercontent.com/latynt/ans/master/data/stance/test.csv', 'https://raw.githubusercontent.com/latynt/ans/master/data/stance/train.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': {'inputs':[downloaded_files[0]]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': {'inputs':[downloaded_files[1]]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':[downloaded_files[2]]} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 3:
				continue
			df.columns = ['s1', 's2', 'stance']
			for _, record in df.iterrows():
				yield str(_id), {'s1':record['s1'],'s2':record['s2'],'label':str(record['stance'])}
				_id += 1 

