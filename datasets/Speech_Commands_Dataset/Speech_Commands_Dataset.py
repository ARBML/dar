import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class Speech_Commands_Dataset(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'file':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['start', 'disable', 'enter', 'enable', 'next', 'rotate', 'ok', 'digit', 'no', 'yes', 'undo', 'up', 'right', 'record', 'one', 'five', 'left', 'down', 'move', 'four', 'six', 'zoom in', 'send', 'zero', 'close', 'direction', 'eight', 'two', 'open', 'seven', 'nine', 'cancel', 'backward', 'zoom out', 'previous', 'receive', 'three', 'options', 'forward', 'stop'])}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff', '.wav', '.mp3']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://github.com/abdulkaderghandoura/arabic-speech-commands-dataset/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['arabic-speech-commands-dataset-master/test.csv']]}),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['arabic-speech-commands-dataset-master/train.csv']]})]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 2:
				continue
			df.columns = ['file', 'class']
			for _, record in df.iterrows():
				yield str(_id), {'file':record['file'],'label':str(record['class'])}
				_id += 1 

