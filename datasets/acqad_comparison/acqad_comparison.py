import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json
class acqad_comparison(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'_id':datasets.Value('string'),'answer':datasets.Value('string'),'question':datasets.Value('string'),'decomposition':datasets.Value('string'),'context':datasets.Value('string')}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/acqad/acqad_multihop.json')),} })]


	def read_json(self, filepath, json_key, lines = False):
		if json_key:
			data = json.load(open(filepath))
			df = pd.DataFrame(data[json_key]) 
		else:
			df = pd.read_json(filepath, lines=lines)
		return df

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_json(filepath, lines=False, json_key='')
			if len(df.columns) != 5:
				continue
			df = df[['_id', 'answer', 'question', 'decomposition', 'context']]
			for _, record in df.iterrows():
				yield str(_id), {'_id':record['_id'],'answer':record['answer'],'question':record['question'],'decomposition':record['decomposition'],'context':record['context']}
				_id += 1 

