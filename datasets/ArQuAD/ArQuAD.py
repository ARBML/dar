import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ArQuAD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'context':datasets.Value('string'),'question':datasets.Value('string'),'answer_start':datasets.Value('string'),'text':datasets.Value('string'),'title':datasets.Value('string'),'id':datasets.Value('string')}))

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
		url = ['https://github.com/RashaMObeidat/ArQuAD/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'ArQuAD-main/ArQuAD-train.csv'),]} }),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'ArQuAD-main/ArQuAD-dev.csv'),]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'ArQuAD-main/ArQuAD-test.csv'),]} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r',', skiprows = 0, error_bad_lines = False, header = 0, encoding = 'latin-1')
			if len(df.columns) != 6:
				continue
			df = df[['context', 'question', 'answer_start', 'text', 'title', 'id']]
			for _, record in df.iterrows():
				yield str(_id), {'context':record['context'],'question':record['question'],'answer_start':record['answer_start'],'text':record['text'],'title':record['title'],'id':record['id']}
				_id += 1 

