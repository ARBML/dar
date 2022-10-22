import os
import pandas as pd 
import datasets

class Author_Attribution_Tweets(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet':datasets.Value('string'),'author':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://fada.birzeit.edu/bitstream/20.500.11889/6743/2/AuthorAttributionTweetsTestDataYara_2_Nataly.xlsx', 'https://fada.birzeit.edu/bitstream/20.500.11889/6743/1/AuthorAttributionTweetsTrainingDataYara_2_Nataly.xlsx']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[1]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = None)
			df.columns = ['tweet', 'author']
			for _, record in df.iterrows():
				yield str(_id), {'tweet':record['tweet'],'author':record['author']}
				_id += 1 

