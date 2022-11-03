import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class Arabic_RC_Trec(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'question_id':datasets.Value('string'),'question':datasets.Value('string'),'answer':datasets.Value('string'),'passage':datasets.Value('string'),'question_class':datasets.Value('string'),'question_subclass':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/MariamBiltawi/Arabic_RC_datasets/main/data/updated_Trec_dataset.xlsx']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 6:
				continue
			df.columns = ['question_id', 'question', 'answer', 'passage', 'question_class', 'question_subclass']
			for _, record in df.iterrows():
				yield str(_id), {'question_id':record['question_id'],'question':record['question'],'answer':record['answer'],'passage':record['passage'],'question_class':record['question_class'],'question_subclass':record['question_subclass']}
				_id += 1 

