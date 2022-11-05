import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class OSAC_CNN(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'articles':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['scitech', 'entertainment', 'sport', 'business', 'middle_east', 'world'])}))

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
		url = ['https://sourceforge.net/projects/ar-text-mining/files/Arabic-Corpora/cnn-arabic-utf8.7z']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/cnn-arabic-utf8/**/**.txt')),} })]


	def get_label_from_path(self, labels, label):
		for l in labels:
			if l == label:
				return label

	def read_txt(self, filepath, skiprows = 0, lines = True):
		if lines:
			return pd.DataFrame(open(filepath, 'r').read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r').read()])

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = False)
			if len(df.columns) != 1:
				continue
			df.columns = ['articles']
			label = self.get_label_from_path(['scitech', 'entertainment', 'sport', 'business', 'middle_east', 'world'], filepath.split('/')[-2])
			for _, record in df.iterrows():
				yield str(_id), {'articles':record['articles'],'label':str(label)}
				_id += 1 

