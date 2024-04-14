import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class AyaTec(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Question':datasets.Value('string')}))

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
		url = ['https://gitlab.com/bigirqu/quran-qa-2023/-/archive/main/main.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'quran-qa-2023-main-21a14601d4599df8e163d26b09cbff79a06628e8/Task-A/data/QQA23_TaskA_ayatec_v1.2_dev.tsv'),]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'quran-qa-2023-main-21a14601d4599df8e163d26b09cbff79a06628e8/Task-A/data/QQA23_TaskA_ayatec_v1.2_test.tsv'),]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'quran-qa-2023-main-21a14601d4599df8e163d26b09cbff79a06628e8/Task-A/data/QQA23_TaskA_ayatec_v1.2_train.tsv'),]} })]


	def read_txt(self, filepath, skiprows = 0, lines = True, encoding = 'utf-8'):
		if lines:
			return pd.DataFrame(open(filepath, 'r', encoding = encoding).read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r', encoding = encoding).read()])

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = True, encoding = 'utf-8')
			if len(df.columns) != 1:
				continue
			df.columns = ['Question']
			df = df[['Question']]
			for _, record in df.iterrows():
				yield str(_id), {'Question':record['Question']}
				_id += 1 

