import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ArCorona(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'created_at':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['RumorOrRefuteRumor', 'AdviceOrCaution', 'Report', 'StoryOrOpinion', 'SeekAction', 'CureOrDiagnosis', 'Prayer', 'SupportOrPraise', 'Info', 'VolunteeringOrDonation', 'MeasureOrAction', 'UnrelatedOrUnimportant', 'NotArabic'])}))

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
		url = ['https://alt.qcri.org/resources/ArCorona.tsv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 3:
				continue
			df.columns = ['id', 'created_at', 'category']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'created_at':record['created_at'],'label':str(record['category'])}
				_id += 1 

