import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class twitter_flood_detection(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ID':datasets.Value('string'),'Tweet Text':datasets.Value('string'),'Relevance':datasets.Value('string'),'Information Type':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Jordan floods', 'Kuwait floods', 'Qurayyat floods', 'Al Lith floods'])}))

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
		url = ['https://raw.githubusercontent.com/alaa-a-a/Arabic-Twitter-corpus-for-flood-detection/master/ArabicFloods.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 5:
				continue
			df.columns = ['ID', 'Tweet Text', 'Relevance', 'Information Type', 'Event']
			for _, record in df.iterrows():
				yield str(_id), {'ID':record['ID'],'Tweet Text':record['Tweet Text'],'Relevance':record['Relevance'],'Information Type':record['Information Type'],'label':str(record['Event'])}
				_id += 1 

