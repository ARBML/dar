import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import re
from bs4 import BeautifulSoup
class CORMAP(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['دارجة', 'خليط', 'عربية'])}))

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
		url = [os.path.abspath(os.path.expanduser(dl_manager.manual_dir))]
		downloaded_files = url
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
			df = self.read_xml(filepath, ['text', 'lang'])
			if len(df.columns) != 2:
				continue
			df = df[['text', 'lang']]
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'label':str(record['lang'])}
				_id += 1 

