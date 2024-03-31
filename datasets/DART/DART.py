import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class DART(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'score(/3)':datasets.Value('string'),'tweet_Id':datasets.Value('string'),'tweet_text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['MGH', 'LEV', 'EGY', 'IRQ', 'GLF'])}))

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
		url = ['https://www.dropbox.com/s/jslg6fzxeu47flu/dart.zip?dl=1']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/DART/cf-data/**.txt')),} })]


	def get_label_from_path(self, labels, label):
		for l in labels:
			if l == label:
				return label

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			if len(df.columns) != 3:
				continue
			df.columns = ['score(/3)', 'tweet_Id', 'tweet_text']
			label = self.get_label_from_path(['MGH', 'LEV', 'EGY', 'IRQ', 'GLF'], filepath.split('/')[-1].split('.')[-2])
			for _, record in df.iterrows():
				yield str(_id), {'score(/3)':record['score(/3)'],'tweet_Id':record['tweet_Id'],'tweet_text':record['tweet_text'],'label':str(label)}
				_id += 1 

