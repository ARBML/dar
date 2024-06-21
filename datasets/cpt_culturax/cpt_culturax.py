import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class cpt_culturax(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'url':datasets.Value('string'),'text':datasets.Value('string'),'cleaned_text':datasets.Value('string'),'filter':datasets.Value('string')}))

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
		url = ['https://docs.google.com/spreadsheets/d/1S-vATi29FFviSQ5kmGvbVqP2N_G-IJqJntSoLgyg_ho/gviz/tq?tqx=out:csv&sheet=Sheet1']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r',', skiprows = 0, error_bad_lines = False, header = 0, encoding = 'utf-8')
			if len(df.columns) != 26:
				continue
			df = df[['id', 'url', 'text', 'cleaned_text', 'filter']]
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'url':record['url'],'text':record['text'],'cleaned_text':record['cleaned_text'],'filter':record['filter']}
				_id += 1 

