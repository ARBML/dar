import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class sudanese_dialect_speech(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'path':datasets.Value('string'),'audio':datasets.Audio(sampling_rate=16_000)}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'wav', 'mp3']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://www.zenodo.org/record/6869079/files/Sudanese_dialect_speech_dataset.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/SDN Dialect Corpus v1.0/Dataset files/**.txt')),'targets1':sorted(glob(downloaded_files[0]+'/SDN Dialect Corpus v1.0/Dataset files/**.wav')),} })]


	def read_wav(self, filepath):
		if filepath.endswith('.wav') or filepath.endswith('.mp3'):
			raw_data = {'filepath':[filepath], 'audio':[filepath]}
		else:
			raw_data = {'text':[open(filepath).read()]}
		return pd.DataFrame(raw_data)

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_wav(filepath)
			dfs = [df] 
			dfs.append(self.read_wav(filepaths['targets1'][i]))
			df = pd.concat(dfs, axis = 1)
			if len(df.columns) != 3:
				continue
			df.columns = ['text', 'path', 'audio']
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'path':record['path'],'audio':record['audio']}
				_id += 1 

