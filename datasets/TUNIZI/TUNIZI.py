import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'label':datasets.features.ClassLabel(names=['negative', 'positive']),'sentence':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/chaymafourati/TUNIZI-Sentiment-Analysis-Tunisian-Arabizi-Dataset/master/TUNIZI-Dataset.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ';', skiprows = 0, error_bad_lines = False)
			df.columns = ['label', 'sentence']
			for _, record in df.iterrows():
				if record['label'] not in ['1', '-1']:
					continue
				yield str(_id), {'label':record['label'],'sentence':record['sentence']}
				_id += 1 

