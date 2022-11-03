import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ARCD_WMI(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'SentenceID':datasets.Value('string'),'Value (Sentence)':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[1, 2, 3])}))

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
		url = ['https://raw.githubusercontent.com/iwan-rg/ARC-WMI/master/ARC-WMI%20sample1.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 9, error_bad_lines = False, header = 0)
			if len(df.columns) != 3:
				continue
			df.columns = ['SentenceID', 'Value (Sentence)', 'annotation value']
			for _, record in df.iterrows():
				yield str(_id), {'SentenceID':record['SentenceID'],'Value (Sentence)':record['Value (Sentence)'],'label':str(record['annotation value'])}
				_id += 1 

