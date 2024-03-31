import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class CoronaVirus(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet_id':datasets.Value('string')}))

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
		url = ['https://github.com/aseelad/Coronavirus-Public-Arabic-Twitter-Data-Set/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/Coronavirus-Public-Arabic-Twitter-Data-Set-master/**/**.txt')),} })]


	def read_txt(self, filepath, skiprows = 0, lines = True, encoding = 'utf-8'):
		if lines:
			return pd.DataFrame(open(filepath, 'r', encoding = encoding).read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r', encoding = encoding).read()])

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = True, encoding = 'utf-8')
			if len(df.columns) != 1:
				continue
			df.columns = ['tweet_id']
			for _, record in df.iterrows():
				yield str(_id), {'tweet_id':record['tweet_id']}
				_id += 1 

