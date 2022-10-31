import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class CAPT(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'file_name':datasets.Value('string'),'pronouced_word':datasets.Value('string'),'pronouced_word_Arabic _letters':datasets.Value('string'),'speaker':datasets.Value('string'),'evaluation':datasets.Value('string'),'folder':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/bhalima/Arabic-Dataset-for-CAPT/main/corpus.rar']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		files = self.get_all_files(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': files})]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 6:
				continue
			df.columns = ['file_name', 'pronouced_word', 'pronouced_word_Arabic _letters', 'speaker', 'evaluation', 'folder']
			for _, record in df.iterrows():
				yield str(_id), {'file_name':record['file_name'],'pronouced_word':record['pronouced_word'],'pronouced_word_Arabic _letters':record['pronouced_word_Arabic _letters'],'speaker':record['speaker'],'evaluation':record['evaluation'],'folder':record['folder']}
				_id += 1 

