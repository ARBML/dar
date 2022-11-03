import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class Syria_Tweet_Sentiment(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'Arabic_txt':datasets.Value('string'),'Ar:man_confidence':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Negative', 'Positive', 'Neutral'])}))

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
		url = ['https://saifmohammad.com/WebDocs/Arabic-Sentiment-Corpora/syr_twts%20_shared.xlsx']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 4:
				continue
			df.columns = ['id', 'Arabic_txt', 'Ar:man_sentiment', 'Ar:man_confidence']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'Arabic_txt':record['Arabic_txt'],'Ar:man_confidence':record['Ar:man_confidence'],'label':str(record['Ar:man_sentiment'])}
				_id += 1 

