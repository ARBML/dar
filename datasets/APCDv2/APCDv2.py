import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class APCDv2(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'poem':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['الرجز', 'المضارع', 'الخفيف', 'الطويل', 'المتدارك', 'نثر', 'البسيط', 'المتقارب', 'الهزج', 'المقتضب', 'الرمل', 'السريع', 'المنسرح', 'المديد', 'الكامل', 'المجتث', 'الوافر'])}))

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
		url = ['https://raw.githubusercontent.com/Gheith-Abandah/classify-arabic-poetry/master/APCD_plus_porse_all.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		files = self.get_all_files(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			if len(df.columns) != 2:
				continue
			df.columns = ['poem', 'meter']
			for _, record in df.iterrows():
				yield str(_id), {'poem':record['poem'],'label':str(record['meter'])}
				_id += 1 

