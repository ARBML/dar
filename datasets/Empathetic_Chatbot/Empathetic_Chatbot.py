import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class Empathetic_Chatbot(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'context':datasets.Value('string'),'response':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['nostalgic', 'sentimental', 'disappointed', 'joyful', 'trusting', 'surprised', 'faithful', 'embarrassed', 'annoyed', 'furious', 'devastated', 'impressed', 'caring', 'terrified', 'afraid', 'disgusted', 'lonely', 'grateful', 'anticipating', 'ashamed', 'proud', 'hopeful', 'prepared', 'angry', 'jealous', 'anxious', 'excited', 'confident', 'content', 'apprehensive', 'sad', 'guilty'])}))

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
		url = ['https://raw.githubusercontent.com/aub-mind/Arabic-Empathetic-Chatbot/master/arabic-empathetic-conversations.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 3:
				continue
			df.columns = ['emotion', 'context', 'response']
			for _, record in df.iterrows():
				yield str(_id), {'context':record['context'],'response':record['response'],'label':str(record['emotion'])}
				_id += 1 

