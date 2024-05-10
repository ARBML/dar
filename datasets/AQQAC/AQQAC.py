import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import re
from bs4 import BeautifulSoup
class AQQAC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Question':datasets.Value('string'),'Answer':datasets.Value('string'),'q_SRC_id':datasets.Value('string'),'Source':datasets.Value('string'),'Quetion_type':datasets.Value('string')}))

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
		url = ['https://archive.researchdata.leeds.ac.uk/464/1/AAQQAC.XML']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]



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
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_xml(filepath, ['Question', 'Answer', 'q_SRC_id', 'Source', 'Quetion_type'])
			if len(df.columns) != 5:
				continue
			df = df[['Question', 'Answer', 'q_SRC_id', 'Source', 'Quetion_type']]
			for _, record in df.iterrows():
				yield str(_id), {'Question':record['Question'],'Answer':record['Answer'],'q_SRC_id':record['q_SRC_id'],'Source':record['Source'],'Quetion_type':record['Quetion_type']}
				_id += 1 

