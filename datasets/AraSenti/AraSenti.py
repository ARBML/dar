import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class AraSenti(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Word':datasets.Value('string'),'Score':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/nora-twairesh/AraSenti/master/AraSentiLexiconV1.0']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r' ', skiprows = 69, error_bad_lines = False, header = None, encoding = 'latin-1')
			if len(df.columns) != 4:
				continue
			df.columns = ['Word', 'e1', 'e2', 'Score']
			df = df[['Word', 'Score']]
			for _, record in df.iterrows():
				yield str(_id), {'Word':record['Word'],'Score':record['Score']}
				_id += 1 

