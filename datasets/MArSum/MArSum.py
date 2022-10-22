import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'summary':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/KamelGaanoun/MoroccanSummarization/main/MArSum1_test.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[0]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ' ', skiprows = 0, error_bad_lines = False)
			df.columns = ['text', 'summary']
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'summary':record['summary']}
				_id += 1 

