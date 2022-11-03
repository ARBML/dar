import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class APCD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'era':datasets.Value('string'),'poet':datasets.Value('string'),'diwan':datasets.Value('string'),'qafiyah':datasets.Value('string'),'meter':datasets.Value('string'),'first_shatr':datasets.Value('string'),'second_shatr':datasets.Value('string'),'bayt':datasets.Value('string')}))

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
		url = ['https://drive.google.com/uc?export=download&id=1xcb2p_TsQbexX9TIxfKMlLLcJvb-Dmly']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		files = self.get_all_files(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 8:
				continue
			df.columns = ['era', 'poet', 'diwan', 'qafiyah', 'meter', 'first_shatr', 'second_shatr', 'bayt']
			for _, record in df.iterrows():
				yield str(_id), {'era':record['era'],'poet':record['poet'],'diwan':record['diwan'],'qafiyah':record['qafiyah'],'meter':record['meter'],'first_shatr':record['first_shatr'],'second_shatr':record['second_shatr'],'bayt':record['bayt']}
				_id += 1 

