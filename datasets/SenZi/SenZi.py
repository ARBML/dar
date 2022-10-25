import os
import pandas as pd 
import datasets

class SenZi(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet_filter':datasets.Value('string'),'arabizi':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0, 1])}))
	def _split_generators(self, dl_manager):
		url = ['https://tahatobaili.github.io/project-rbz/resources/arabizi-identification.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Arabizi Identification/arabizi-twitter-leb.csv', 'Arabizi Identification/arabizi-twitter-egy.csv']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = [0, 1]
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['tweet_filter', 'arabizi']
			for _, record in df.iterrows():
				yield str(_id), {'tweet_filter':record['tweet_filter'],'arabizi':record['arabizi'],'label':str(labels[i])}
				_id += 1 

