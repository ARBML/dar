import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class wiki_lingua_ar(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'article':datasets.Value('string'),'summary':datasets.Value('string')}))

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
		url = ['https://drive.google.com/uc?id=1PM7GFCy2gJL1WHqQz1dzqIDIEN6kfRoi']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'WikiLingua_data_splits/arabic/test.src.ar'),],'targets1':[os.path.join(downloaded_files[0],'WikiLingua_data_splits/arabic/test.tgt.ar'),]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'WikiLingua_data_splits/arabic/train.src.ar'),],'targets1':[os.path.join(downloaded_files[0],'WikiLingua_data_splits/arabic/train.tgt.ar'),]} }),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'WikiLingua_data_splits/arabic/val.src.ar'),],'targets1':[os.path.join(downloaded_files[0],'WikiLingua_data_splits/arabic/val.tgt.ar'),]} })]


	def read_txt(self, filepath, skiprows = 0, lines = True):
		if lines:
			return pd.DataFrame(open(filepath, 'r').read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r').read()])

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = True)
			dfs = [df] 
			dfs.append(self.read_txt(filepaths['targets1'][i], skiprows = 0, lines = True))
			df = pd.concat(dfs, axis = 1)
			if len(df.columns) != 2:
				continue
			df.columns = ['article', 'summary']
			for _, record in df.iterrows():
				yield str(_id), {'article':record['article'],'summary':record['summary']}
				_id += 1 

