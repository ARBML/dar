import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class TUNIZI(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'sentiment':datasets.Value('string'),'text':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/chaymafourati/TUNIZI-Sentiment-Analysis-Tunisian-Arabizi-Dataset/master/TUNIZI-Dataset.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r';', skiprows = 0, error_bad_lines = False, header = None)
			if len(df.columns) != 2:
				continue
			df.columns = ['sentiment', 'text']
			for _, record in df.iterrows():
				yield str(_id), {'sentiment':record['sentiment'],'text':record['text']}
				_id += 1
