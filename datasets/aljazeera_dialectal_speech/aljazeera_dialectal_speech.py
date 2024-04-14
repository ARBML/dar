import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class aljazeera_dialectal_speech(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Url':datasets.Value('string'),'Start timestamp':datasets.Value('string'),'Stop timestamp':datasets.Value('string'),'Speaker ID':datasets.Value('string'),'Label':datasets.Value('string'),'Confidence':datasets.Value('string')}))

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
		url = ['https://alt.qcri.org/wp-content/uploads/2020/08/is_speechcorpus.txt']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			if len(df.columns) != 6:
				continue
			df = df[['Url', 'Start timestamp', 'Stop timestamp', 'Speaker ID', 'Label', 'Confidence']]
			for _, record in df.iterrows():
				yield str(_id), {'Url':record['Url'],'Start timestamp':record['Start timestamp'],'Stop timestamp':record['Stop timestamp'],'Speaker ID':record['Speaker ID'],'Label':record['Label'],'Confidence':record['Confidence']}
				_id += 1 

