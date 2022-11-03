import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ArCovidVac(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'split':datasets.Value('string'),'text':datasets.Value('string'),'category':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0, 1, -1])}))

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
		url = ['https://alt.qcri.org/resources/ArCovidVac.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		files = self.get_all_files(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 4:
				continue
			df.columns = ['split', 'text', 'category', 'stance']
			for _, record in df.iterrows():
				yield str(_id), {'split':record['split'],'text':record['text'],'category':record['category'],'label':str(record['stance'])}
				_id += 1 

