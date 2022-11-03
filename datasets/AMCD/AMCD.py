import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class AMCD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Comment':datasets.Value('string'),'Videl_desc':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[1, 2, 3, 4])}))

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
		url = ['https://raw.githubusercontent.com/waelyafooz/Arabic-Multi-Classification-Dataset-AMCD/main/AMCD%20Ver%200.1.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 3:
				continue
			df.columns = ['Comment', 'Class', 'Videl_desc']
			for _, record in df.iterrows():
				yield str(_id), {'Comment':record['Comment'],'Videl_desc':record['Videl_desc'],'label':str(record['Class'])}
				_id += 1 

