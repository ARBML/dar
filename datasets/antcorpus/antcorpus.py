import os
import pandas as pd 
import datasets
from bs4 import BeautifulSoup
import re
from glob import glob
class antcorpus(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'TEXT':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['culture', 'internationalNews', 'sport', 'diverse', 'society', 'technology', 'politic', 'localNews', 'economy'])}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/antcorpus/antcorpus.data/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(f'{downloaded_files[0]}/antcorpus.data-master/**/**.txt')})]


	def get_data(self, bs, column):
		elements =  [attr[column] for attr in bs.find_all(attrs={column : re.compile(".")})]
		if len(elements) == 0:
			elements = [el.text for el in bs.find_all(column)]
		return elements

	def read_xml(self, path, columns):
		with open(path, 'rb') as f:
			data = f.read()
    
		bs = BeautifulSoup(data, "xml")
		data = {}
		for column in columns:
			elements = self.get_data(bs, column)
			data[column] = elements
		return pd.DataFrame(data)
	def get_label_from_path(self, labels, path):
		for label in labels:
			if label in path:
				return label
		raise('error')
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = self.read_xml(filepath, ['TEXT'])
			df.columns = ['TEXT']
			label = self.get_label_from_path(['culture', 'internationalNews', 'sport', 'diverse', 'society', 'technology', 'politic', 'localNews', 'economy'], filepath)
			for _, record in df.iterrows():
				yield str(_id), {'TEXT':record['TEXT'],'label':str(label)}
				_id += 1 

