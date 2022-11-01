import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class KalamDZ(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'path':datasets.Value('string'),'audio':datasets.Audio(sampling_rate=16_000),'label': datasets.features.ClassLabel(names=['Sulaymite', 'Maaqilian', 'Hilali-Saharan', 'Sahel-Tell', 'Algiers Blanks', 'Pre-hilalien', 'High-plains', 'Hilali-Tellian'])}))

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
		url = ['https://raw.githubusercontent.com/LIM-MoDos/KalamDZ/master/Data.rar']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(downloaded_files[0]+'/Data/**/**/**.wav')})]


	def get_label_from_path(self, labels, path):
		for label in labels:
			if label in path:
				return label

	def read_wav(self, filepath):
		raw_data = {'filepath':[filepath], 'audio':[filepath]}
		return pd.DataFrame(raw_data)
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = self.read_wav(filepath)
			if len(df.columns) != 2:
				continue
			df.columns = ['path', 'audio']
			label = self.get_label_from_path(['Sulaymite', 'Maaqilian', 'Hilali-Saharan', 'Sahel-Tell', 'Algiers Blanks', 'Pre-hilalien', 'High-plains', 'Hilali-Tellian'], filepath)
			for _, record in df.iterrows():
				yield str(_id), {'path':record['path'],'audio':record['audio'],'label':str(label)}
				_id += 1 

