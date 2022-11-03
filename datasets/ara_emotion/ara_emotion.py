import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class ara_emotion(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet_id':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['disgust', 'fear', 'anticipation', 'trust', 'surprise', 'anger', 'happiness', 'sadness'])}))

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
		url = ['https://raw.githubusercontent.com/UBC-NLP/ara_emotion_naacl2018/master/data_splits/lama_dev.csv', 'https://raw.githubusercontent.com/UBC-NLP/ara_emotion_naacl2018/master/data_splits/lama_test.csv', 'https://raw.githubusercontent.com/UBC-NLP/ara_emotion_naacl2018/master/data_splits/lama_train.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': {'inputs':[downloaded_files[0]]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': {'inputs':[downloaded_files[1]]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':[downloaded_files[2]]} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 2:
				continue
			df.columns = ['tweet_id', 'label']
			for _, record in df.iterrows():
				yield str(_id), {'tweet_id':record['tweet_id'],'label':str(record['label'])}
				_id += 1 

