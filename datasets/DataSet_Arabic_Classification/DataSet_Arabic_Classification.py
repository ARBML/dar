import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class DataSet_Arabic_Classification(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0, 1, 2, 3, 4])}))

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
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/v524p5dhpj-2.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		files = self.get_all_files(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': files})]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 2:
				continue
			df.columns = ['text', 'target']
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'label':str(record['target'])}
				_id += 1 

