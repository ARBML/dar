import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class news_articles_aljazeera(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'guid':datasets.Value('string'),'published':datasets.Value('string'),'title':datasets.Value('string'),'description':datasets.Value('string'),'link':datasets.Value('string'),'content':datasets.Value('string'),'image_url':datasets.Value('string'),'ref':datasets.Value('string'),'tags':datasets.Value('string')}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/**.csv')),} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 9:
				continue
			df.columns = ['guid', 'published', 'title', 'description', 'link', 'content', 'image_url', 'ref', 'tags']
			for _, record in df.iterrows():
				yield str(_id), {'guid':record['guid'],'published':record['published'],'title':record['title'],'description':record['description'],'link':record['link'],'content':record['content'],'image_url':record['image_url'],'ref':record['ref'],'tags':record['tags']}
				_id += 1 

