import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class AQweets(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'tweet':datasets.Value('string'),'score':datasets.Value('string')}))

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


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = None, engine = 'python', encoding = 'utf-8')
			if len(df.columns) != 3:
				continue
			df.columns = ['id', 'tweet', 'score']
			df = df[['id', 'tweet', 'score']]
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'tweet':record['tweet'],'score':record['score']}
				_id += 1 

