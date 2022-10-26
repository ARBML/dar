import os
import pandas as pd 
import datasets

class Dangerous_Dataset(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Tweet':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['safe', 'dangerous'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/UBC-NLP/Arabic-Dangerous-Dataset/master/Dangerous_Dataset.tsv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['safe', 'dangerous']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			df.columns = ['Tweet', 'Label']
			for _, record in df.iterrows():
				yield str(_id), {'Tweet':record['Tweet'],'label':str(record['Label'])}
				_id += 1 

