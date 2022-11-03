import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class AKEC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'lemmatized':datasets.Value('string'),'pure':datasets.Value('string')}))

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
		url = ['https://github.com/ailab-uniud/akec/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':sorted(glob(downloaded_files[0]+'/akec-master/keyphrases/sort_frequency/lemmatized.txt')),'targets':sorted(glob(downloaded_files[0]+'/akec-master/keyphrases/sort_frequency/pure.txt'))} })]


	def read_txt(self, filepath, skiprows = 0, lines = True):
		if lines:
			return pd.DataFrame(open(filepath, 'r').read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r').read()])
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = True)
			df1 = self.read_txt(filepaths['targets'][i], skiprows = 0, lines = True)
			df = pd.concat([df,df1], axis = 1)
			if len(df.columns) != 2:
				continue
			df.columns = ['lemmatized', 'pure']
			for _, record in df.iterrows():
				yield str(_id), {'lemmatized':record['lemmatized'],'pure':record['pure']}
				_id += 1 

