import os
import pandas as pd 
import datasets

class AraSenti_Lexicon(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Term':datasets.Value('string'),'Sentiment':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/nora-twairesh/AraSenti/master/AraSentiLexiconV1.0']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '   ', skiprows = 70, error_bad_lines = False, header = None,encoding= 'unicode_escape')
			df.columns = ['Term', 'Sentiment']
			for _, record in df.iterrows():
				yield str(_id), {'Term':record['Term'],'Sentiment':record['Sentiment']}
				_id += 1 

