import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class SANAD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Article':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Tech', 'Finance', 'Politics', 'Religion', 'Medical', 'Culture', 'Sports'])}))

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
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/57zpx667y9-2.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/**/**.txt')),} })]


	def get_label_from_path(self, labels, label):
		for l in labels:
			if l == label:
				return label

	def read_txt(self, filepath, skiprows = 0, lines = True, encoding = 'utf-8'):
		if lines:
			return pd.DataFrame(open(filepath, 'r', encoding = encoding).read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r', encoding = encoding).read()])

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = False, encoding = 'utf-8')
			if len(df.columns) != 1:
				continue
			df.columns = ['Article']
			df = df[['Article']]
			label = self.get_label_from_path(['Tech', 'Finance', 'Politics', 'Religion', 'Medical', 'Culture', 'Sports'], filepath.split('/')[-2])
			for _, record in df.iterrows():
				yield str(_id), {'Article':record['Article'],'label':str(label)}
				_id += 1 

