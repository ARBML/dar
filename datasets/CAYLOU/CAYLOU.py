import os
import pandas as pd 
import datasets
import re
from bs4 import BeautifulSoup
class CAYLOU(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Source':datasets.Value('string'),'Target':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/abidikarima/CALYOU/master/CALYOU.v1.xml']
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
			df = self.read_xml(filepath, ['Source', 'Target'])
			df.columns = ['Source', 'Target']
			for _, record in df.iterrows():
				yield str(_id), {'Source':record['Source'],'Target':record['Target']}
				_id += 1 

