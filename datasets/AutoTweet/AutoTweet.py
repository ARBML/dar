import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class AutoTweet(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'TweetID':datasets.Value('string'),'TweetText':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Manual', 'Automated'])}))

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
		url = ['https://www.dropbox.com/s/amnv06boef2vn4k/autotweet-dataset-v1.0.zip?dl=1']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/Autotweet-Dataset-v1.0/Autotweet-v1.0-Labeled-Tweets-Text.txt')),} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			if len(df.columns) != 3:
				continue
			df.columns = ['TweetID', 'TweetText', 'Label']
			for _, record in df.iterrows():
				yield str(_id), {'TweetID':record['TweetID'],'TweetText':record['TweetText'],'label':str(record['Label'])}
				_id += 1 

