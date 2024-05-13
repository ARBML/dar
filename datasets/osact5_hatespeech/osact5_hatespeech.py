import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class osact5_hatespeech(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'tweet':datasets.Value('string'),'off':datasets.Value('string'),'hs':datasets.Value('string'),'vlg':datasets.Value('string'),'vio':datasets.Value('string')}))

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
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'OSACT2022-sharedTask-dev.txt'),]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'OSACT2022-sharedTask-train.txt'),]} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python', encoding = 'utf-8')
			if len(df.columns) != 6:
				continue
			df = df[['id', 'tweet', 'off', 'hs', 'vlg', 'vio']]
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'tweet':record['tweet'],'off':record['off'],'hs':record['hs'],'vlg':record['vlg'],'vio':record['vio']}
				_id += 1 

