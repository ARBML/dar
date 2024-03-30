import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class arabic_100k_reviews(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Positive', 'Mixed', 'Negative'])}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/ar_reviews_100k.tsv')),} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			if len(df.columns) != 2:
				continue
			df.columns = ['label', 'text']
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'label':str(record['label'])}
				_id += 1 

