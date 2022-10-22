import os
import pandas as pd 
import datasets
import re
from bs4 import BeautifulSoup
class PADIC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ALGIERS':datasets.Value('string'),'ANNABA':datasets.Value('string'),'MODERN-STANDARD-ARABIC':datasets.Value('string'),'SYRIAN':datasets.Value('string'),'PALESTINIAN':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://sourceforge.net/projects/padic/files/PADIC.xml']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]


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
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = self.read_xml(filepath, ['ALGIERS', 'ANNABA', 'MODERN-STANDARD-ARABIC', 'SYRIAN', 'PALESTINIAN'])
			df.columns = ['ALGIERS', 'ANNABA', 'MODERN-STANDARD-ARABIC', 'SYRIAN', 'PALESTINIAN']
			for _, record in df.iterrows():
				yield str(_id), {'ALGIERS':record['ALGIERS'],'ANNABA':record['ANNABA'],'MODERN-STANDARD-ARABIC':record['MODERN-STANDARD-ARABIC'],'SYRIAN':record['SYRIAN'],'PALESTINIAN':record['PALESTINIAN']}
				_id += 1 

