import os
import pandas as pd 
import datasets

class CTAB(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'sentence':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://zenodo.org/record/4781769/files/CTAB-SAMPLE0001.txt','https://zenodo.org/record/4781769/files/CTAB-SAMPLE0002.txt','https://zenodo.org/record/4781769/files/CTAB-SAMPLE0003.txt',]
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		print(filepaths)
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['sentence']
			for _, record in df.iterrows():
				yield str(_id), {'sentence':record['sentence']}
				_id += 1 

