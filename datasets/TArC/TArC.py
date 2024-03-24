import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class TArC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'TYPE':datasets.Value('string'),'data':datasets.Value('string'),'arabish':datasets.Value('string'),'class':datasets.Value('string'),'CODA':datasets.Value('string'),'token':datasets.Value('string'),'pos':datasets.Value('string'),'lemma':datasets.Value('string'),'governorate':datasets.Value('string'),'age':datasets.Value('string'),'gender':datasets.Value('string')}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'wav', 'mp3', 'jpg', 'png']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/eligugliotta/tarc/master/tarc.tsv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			if len(df.columns) != 11:
				continue
			df.columns = ['TYPE', 'data', 'arabish', 'class', 'CODA', 'token', 'pos', 'lemma', 'governorate', 'age', 'gender']
			for _, record in df.iterrows():
				yield str(_id), {'TYPE':record['TYPE'],'data':record['data'],'arabish':record['arabish'],'class':record['class'],'CODA':record['CODA'],'token':record['token'],'pos':record['pos'],'lemma':record['lemma'],'governorate':record['governorate'],'age':record['age'],'gender':record['gender']}
				_id += 1 

