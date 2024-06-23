import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import csv

class a(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Word':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['B-ORG', 'I-MISC', 'B-PERS', 'O', 'B-LOC', 'B-MISC', 'I-LOC', 'I-ORG', 'I-PERS'])}))

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
		url = ['https://github.com/iwan-rg/CLEANANERCorp/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'CLEANANERCorp-main/cleananercorp_TRAIN.txt'),
		]} }), datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'CLEANANERCorp-main/cleananercorp_TEST.txt')]} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r' ', skiprows = 0, error_bad_lines = False, header = None, encoding = 'utf-8', quoting=csv.QUOTE_NONE)
			if len(df.columns) != 2:
				continue
			df.columns = ['Word', 'Entity']
			df = df[['Word', 'Entity']]
			for _, record in df.iterrows():
				yield str(_id), {'Word':record['Word'],'label':str(record['Entity'])}
				_id += 1 

