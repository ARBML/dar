import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class google_transliteration(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ar':datasets.Value('string'),'en':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/google/transliteration/master/ar2en-eval.txt', 'https://raw.githubusercontent.com/google/transliteration/master/ar2en-test.txt', 'https://raw.githubusercontent.com/google/transliteration/master/ar2en-train.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': {'inputs':[downloaded_files[0]]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': {'inputs':[downloaded_files[1]]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':[downloaded_files[2]]} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = None)
			if len(df.columns) != 2:
				continue
			df.columns = ['ar', 'en']
			for _, record in df.iterrows():
				yield str(_id), {'ar':record['ar'],'en':record['en']}
				_id += 1 

