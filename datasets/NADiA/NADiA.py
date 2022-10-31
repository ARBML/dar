import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class NADiA(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string')}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff'] 
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/hhrb7phdyx-1.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(downloaded_files[0]+'/NADiA1/**.txt')})]


	def read_txt(self, filepath, skiprows = 0):
		lines = open(filepath, 'r').read().splitlines()[skiprows:]
		return pd.DataFrame(lines)
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = self.read_txt(filepath, skiprows = 0)
			if len(df.columns) != 1:
				continue
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text']}
				_id += 1 

