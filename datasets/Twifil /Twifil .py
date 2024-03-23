import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class Twifil (datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ID':datasets.Value('string'),'Code':datasets.Value('string'),'Post':datasets.Value('string'),'lang':datasets.Value('string'),'Created At':datasets.Value('string'),'Followers Count':datasets.Value('string'),'Profile Link Color':datasets.Value('string'),'Geo Enabled':datasets.Value('string'),'Screen Name':datasets.Value('string'),'Name':datasets.Value('string'),'Profile Lang':datasets.Value('string'),'Polarity':datasets.Value('string'),'Polarity Class':datasets.Value('string'),'User Age':datasets.Value('string'),'Emotion':datasets.Value('string'),'Platform':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/kinmokusu/oea_algd/master/data/dataset/data.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = r',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 16:
				continue
			df.columns = ['ID', 'Code', 'Post', 'lang', 'Created At', 'Followers Count', 'Profile Link Color', 'Geo Enabled', 'Screen Name', 'Name', 'Profile Lang', 'Polarity', 'Polarity Class', 'User Age', 'Emotion', 'Platform']
			for _, record in df.iterrows():
				yield str(_id), {'ID':record['ID'],'Code':record['Code'],'Post':record['Post'],'lang':record['lang'],'Created At':record['Created At'],'Followers Count':record['Followers Count'],'Profile Link Color':record['Profile Link Color'],'Geo Enabled':record['Geo Enabled'],'Screen Name':record['Screen Name'],'Name':record['Name'],'Profile Lang':record['Profile Lang'],'Polarity':record['Polarity'],'Polarity Class':record['Polarity Class'],'User Age':record['User Age'],'Emotion':record['Emotion'],'Platform':record['Platform']}
				_id += 1 

