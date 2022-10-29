import os
import pandas as pd 
import datasets
from glob import glob

class khaleej_2004(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Sports', 'Economy', 'Local News', 'International news'])}))
	def _split_generators(self, dl_manager):
		url = ['https://sourceforge.net/projects/arabiccorpus/files/arabiccorpus%20%28utf-8%29/Khaleej-2004-utf8.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(downloaded_files[0]+'/Khaleej-2004/**/**.html')})]

	def get_label_from_path(self, labels, path):
		for label in labels:
			if label in path:
				return label

	def read_txt(self, filepath, skiprows = 0):
		lines = open(filepath, 'r').read().splitlines()[skiprows:]
		return pd.DataFrame(lines)
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = self.read_txt(filepath, skiprows = 0)
			df.columns = ['Text']
			label = self.get_label_from_path(['Sports', 'Economy', 'Local News', 'International news'], filepath)
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text'],'label':str(label)}
				_id += 1 

