import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class NileULex(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Term':datasets.Value('string'),'Egyptian/collequial ':datasets.Value('string'),'MSA':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['compound_neg', 'negative', 'positive', 'compound_pos'])}))

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
		url = ['https://raw.githubusercontent.com/NileTMRG/NileULex/master/NileULex_v0.27.xlsx']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 9, header = 0)
			if len(df.columns) != 4:
				continue
			df.columns = ['Term', 'Polarity', 'Egyptian/collequial ', 'MSA']
			for _, record in df.iterrows():
				yield str(_id), {'Term':record['Term'],'Egyptian/collequial ':record['Egyptian/collequial '],'MSA':record['MSA'],'label':str(record['Polarity'])}
				_id += 1 

