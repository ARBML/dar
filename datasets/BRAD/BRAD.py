import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'review_id':datasets.Value('string'),'book_id':datasets.Value('string'),'user_id':datasets.Value('string'),'review':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[1, 2, 3, 4, 5])}))
	def _split_generators(self, dl_manager):
		url = ['https://drive.google.com/uc?export=download&id=1bXJPu_JUnGnillJo8k94mkREmxCNMPQd']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False)
			df.columns = ['rating', 'review_id', 'book_id', 'user_id', 'review']
			for _, record in df.iterrows():
				yield str(_id), {'review_id':record['review_id'],'book_id':record['book_id'],'user_id':record['user_id'],'review':record['review'],'label':str(record['rating'])}
				_id += 1 

