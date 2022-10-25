import os
import pandas as pd 
import datasets

class multilingual_irony(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet_id':datasets.Value('string'),'label':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0, 1])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/bilalghanem/multilingual_irony/master/ECIR_training.csv', 'https://raw.githubusercontent.com/bilalghanem/multilingual_irony/master/ECIR_test.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[1]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = [0, 1]
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['tweet_id', 'label']
			for _, record in df.iterrows():
				yield str(_id), {'tweet_id':record['tweet_id'],'label':record['label'],'label':str(labels[i])}
				_id += 1 

