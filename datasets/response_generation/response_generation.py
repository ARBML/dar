import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class response_generation(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Utterance-LEV':datasets.Value('string'),'Response-LEV':datasets.Value('string'),'Utterance-EGY':datasets.Value('string'),'Response-EGY':datasets.Value('string'),'Utterance-GUL':datasets.Value('string'),'Response-GUL':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/tareknaous/dialogue-arabic-dialects/main/dataset.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 6:
				continue
			df.columns = ['Utterance-LEV', 'Response-LEV', 'Utterance-EGY', 'Response-EGY', 'Utterance-GUL', 'Response-GUL']
			for _, record in df.iterrows():
				yield str(_id), {'Utterance-LEV':record['Utterance-LEV'],'Response-LEV':record['Response-LEV'],'Utterance-EGY':record['Utterance-EGY'],'Response-EGY':record['Response-EGY'],'Utterance-GUL':record['Utterance-GUL'],'Response-GUL':record['Response-GUL']}
				_id += 1 

