import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ArabicTE(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Premise':datasets.Value('string'),'Hypothesis':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[' NotEntails', ' Entails'])}))

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
			df = pd.read_csv(open(filepath, 'rb'), sep = r',', skiprows = 0, error_bad_lines = False, header = 0, encoding = 'utf-8')
			if len(df.columns) != 3:
				continue
			df = df[['Premise', 'Hypothesis', 'Judgement']]
			for _, record in df.iterrows():
				yield str(_id), {'Premise':record['Premise'],'Hypothesis':record['Hypothesis'],'label':str(record['Judgement'])}
				_id += 1 

