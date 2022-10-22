import os
import pandas as pd 
import datasets

class BBN_Blog_Posts(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Arabic_text':datasets.Value('string'),'ar:manual_sentiment':datasets.Value('string'),'ar:manual_confidence':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://saifmohammad.com/WebDocs/Arabic-Sentiment-Corpora/bbn_shared-2.xls']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			df.columns = ['Arabic_text', 'ar:manual_sentiment', 'ar:manual_confidence']
			for _, record in df.iterrows():
				yield str(_id), {'Arabic_text':record['Arabic_text'],'ar:manual_sentiment':record['ar:manual_sentiment'],'ar:manual_confidence':record['ar:manual_confidence']}
				_id += 1 

