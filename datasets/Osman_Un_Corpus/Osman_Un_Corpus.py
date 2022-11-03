import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class Osman_Un_Corpus(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ART':datasets.Value('string'),'AR':datasets.Value('string'),'EN':datasets.Value('string')}))

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
		url = ['https://github.com/drelhaj/OsmanReadability/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/OsmanReadability-master/Osman_UN_Corpus/parallel_documents/ART_docs/**.txt')),'targets1':sorted(glob(downloaded_files[0]+'/OsmanReadability-master/Osman_UN_Corpus/parallel_documents/AR_docs/**.txt')),'targets2':sorted(glob(downloaded_files[0]+'/OsmanReadability-master/Osman_UN_Corpus/parallel_documents/EN_docs/**.txt')),} })]


	def read_txt(self, filepath, skiprows = 0, lines = True):
		if lines:
			return pd.DataFrame(open(filepath, 'r').read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r').read()])
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = False)
			dfs = [df] 
			dfs.append(self.read_txt(filepaths['targets1'][i], skiprows = 0, lines = False))
			dfs.append(self.read_txt(filepaths['targets2'][i], skiprows = 0, lines = False))
			df = pd.concat(dfs, axis = 1)
			if len(df.columns) != 3:
				continue
			df.columns = ['ART', 'AR', 'EN']
			for _, record in df.iterrows():
				yield str(_id), {'ART':record['ART'],'AR':record['AR'],'EN':record['EN']}
				_id += 1 

