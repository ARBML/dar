import os
import pandas as pd 
import datasets
from glob import glob

class Document_Classification(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Health', 'Law', 'Literature', 'Sport', 'Religion', 'Art', 'Economy', 'Technology', 'Politics'])}))
	def _split_generators(self, dl_manager):
		url = ['http://diab.edublogs.org/files/2011/04/dataset-1gwdwo5.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(downloaded_files[0]+'/dataset/version4/**/**.txt')})]

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
			label = self.get_label_from_path(['Health', 'Law', 'Literature', 'Sport', 'Religion', 'Art', 'Economy', 'Technology', 'Politics'], filepath)
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text'],'label':str(label)}
				_id += 1 

