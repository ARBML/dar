import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class arabic_spam_ham_twitter(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Date':datasets.Value('string'),'Time':datasets.Value('string'),'Date Time':datasets.Value('string'),'URL':datasets.Value('string'),'Tweet Text':datasets.Value('string'),'Cleaned Text':datasets.Value('string'),'User Name':datasets.Value('string'),'Location':datasets.Value('string'),'Replied Tweet ID ':datasets.Value('string'),'Replied Tweet User ID':datasets.Value('string'),'Replied Tweet User name':datasets.Value('string'),'Coordinates':datasets.Value('string'),'Retweet Count':datasets.Value('string'),'Favorite Count':datasets.Value('string'),'Favorited':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Ham', 'Spam'])}))

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
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/86x733xkb8-2.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/Dataset of Arabic Spam and Ham Tweets/Dataset of Arabic Spam and Ham Tweets.xlsx')),} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 16:
				continue
			df = df[['Date', 'Time', 'Date Time', 'URL', 'Tweet Text', 'Cleaned Text', 'User Name', 'Location', 'Replied Tweet ID ', 'Replied Tweet User ID', 'Replied Tweet User name', 'Coordinates', 'Retweet Count', 'Favorite Count', 'Favorited', 'Label']]
			for _, record in df.iterrows():
				yield str(_id), {'Date':record['Date'],'Time':record['Time'],'Date Time':record['Date Time'],'URL':record['URL'],'Tweet Text':record['Tweet Text'],'Cleaned Text':record['Cleaned Text'],'User Name':record['User Name'],'Location':record['Location'],'Replied Tweet ID ':record['Replied Tweet ID '],'Replied Tweet User ID':record['Replied Tweet User ID'],'Replied Tweet User name':record['Replied Tweet User name'],'Coordinates':record['Coordinates'],'Retweet Count':record['Retweet Count'],'Favorite Count':record['Favorite Count'],'Favorited':record['Favorited'],'label':str(record['Label'])}
				_id += 1 

