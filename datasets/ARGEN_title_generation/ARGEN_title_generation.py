import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ARGEN_title_generation(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'document':datasets.Value('string'),'title':datasets.Value('string'),'document_count':datasets.Value('string'),'title_count':datasets.Value('string')}))

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
		url = ['https://github.com/UBC-NLP/araT5/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'araT5-main/examples/ARGEn_title_genration_sample_train.tsv'),]} }),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'araT5-main/examples/ARGEn_title_genration_sample_valid.tsv'),os.path.join(downloaded_files[0],'araT5-main/examples/ARGEn_title_genration_sample_valid.tsv'),]} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = '	', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 4:
				continue
			df.columns = ['document', 'title', 'document_count', 'title_count']
			for _, record in df.iterrows():
				yield str(_id), {'document':record['document'],'title':record['title'],'document_count':record['document_count'],'title_count':record['title_count']}
				_id += 1 

