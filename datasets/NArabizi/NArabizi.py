import os
import pandas as pd 
import datasets
class NArabizi(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ID':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['NEU', 'NEG', 'MIX', 'POS'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/SamiaTouileb/NArabizi/main/data/Narabizi/sentiment/dev_Narabizi_sentiment.txt', 'https://raw.githubusercontent.com/SamiaTouileb/NArabizi/main/data/Narabizi/sentiment/test_Narabizi_sentiment.txt', 'https://raw.githubusercontent.com/SamiaTouileb/NArabizi/main/data/Narabizi/sentiment/train_Narabizi_sentiment.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[1]]}),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[2]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['ID', 'SENT']
			for _, record in df.iterrows():
				yield str(_id), {'ID':record['ID'],'label':str(record['SENT'])}
				_id += 1 

