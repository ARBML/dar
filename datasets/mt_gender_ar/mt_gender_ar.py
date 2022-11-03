import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class mt_gender_ar(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Index':datasets.Value('string'),'Entity':datasets.Value('string'),'Sentence':datasets.Value('string'),'Find entity? [Y/N]':datasets.Value('string'),'Gender? [M/F/N]':datasets.Value('string'),'Comments':datasets.Value('string')}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff', 'wav', 'mp3']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/gabrielStanovsky/mt_gender/master/data/human_annotations/aws.ar.in%20-%20ar.in.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 6:
				continue
			df.columns = ['Index', 'Entity', 'Sentence', 'Find entity? [Y/N]', 'Gender? [M/F/N]', 'Comments']
			for _, record in df.iterrows():
				yield str(_id), {'Index':record['Index'],'Entity':record['Entity'],'Sentence':record['Sentence'],'Find entity? [Y/N]':record['Find entity? [Y/N]'],'Gender? [M/F/N]':record['Gender? [M/F/N]'],'Comments':record['Comments']}
				_id += 1 

