import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ArzEn_MultiGenre_songs(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Song-Name':datasets.Value('string'),'Year':datasets.Value('string'),'Album-Name':datasets.Value('string'),'Song-ID':datasets.Value('string'),'Egyptian Arabic Lyrics':datasets.Value('string'),'English Translation':datasets.Value('string')}))

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
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/6k97jty9xg-3.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/**/Songs/*.xlsx')),} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 6:
				continue
			df.columns = ['Song-Name', 'Year', 'Album-Name', 'Song-ID', 'Egyptian Arabic Lyrics', 'English Translation']
			for _, record in df.iterrows():
				yield str(_id), {'Song-Name':record['Song-Name'],'Year':record['Year'],'Album-Name':record['Album-Name'],'Song-ID':record['Song-ID'],'Egyptian Arabic Lyrics':record['Egyptian Arabic Lyrics'],'English Translation':record['English Translation']}
				_id += 1 

