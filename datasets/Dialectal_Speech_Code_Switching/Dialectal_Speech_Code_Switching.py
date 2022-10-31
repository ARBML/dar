import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class Dialectal_Speech_Code_Switching(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'path':datasets.Value('string')}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff'] 
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/qcri/Arabic_speech_code_switching/master/mgb3_audio_list.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]


	def read_txt(self, filepath, skiprows = 0):
		lines = open(filepath, 'r').read().splitlines()[skiprows:]
		return pd.DataFrame(lines)
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = self.read_txt(filepath, skiprows = 0)
			if len(df.columns) != 1:
				continue
			df.columns = ['path']
			for _, record in df.iterrows():
				yield str(_id), {'path':record['path']}
				_id += 1 

