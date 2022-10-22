import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Term':datasets.Value('string'),'bulkwalter':datasets.Value('string'),'sentiment_score':datasets.Value('string'),'positive_occurrence_count':datasets.Value('string'),'negative_occurrence_count':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://saifmohammad.com/WebDocs/Arabic%20Lexicons/Arabic_Emoticon_Lexicon.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 104, error_bad_lines = False)
			df.columns = ['Term', 'bulkwalter', 'sentiment_score', 'positive_occurrence_count', 'negative_occurrence_count']
			for _, record in df.iterrows():
				yield str(_id), {'Term':record['Term'],'bulkwalter':record['bulkwalter'],'sentiment_score':record['sentiment_score'],'positive_occurrence_count':record['positive_occurrence_count'],'negative_occurrence_count':record['negative_occurrence_count']}
				_id += 1 

