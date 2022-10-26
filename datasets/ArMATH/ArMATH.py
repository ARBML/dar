import os
import pandas as pd 
import datasets

class ArMATH(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'question':datasets.Value('string'),'segmented':datasets.Value('string'),'equation':datasets.Value('string'),'tag':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/reem-codes/ArMATH/main/datasets/armath/fold_0.json', 'https://raw.githubusercontent.com/reem-codes/ArMATH/main/datasets/armath/fold_1.json', 'https://raw.githubusercontent.com/reem-codes/ArMATH/main/datasets/armath/fold_2.json', 'https://raw.githubusercontent.com/reem-codes/ArMATH/main/datasets/armath/fold_3.json', 'https://raw.githubusercontent.com/reem-codes/ArMATH/main/datasets/armath/fold_4.json']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_json(open(filepath, 'rb'), lines=False)
			df.columns = ['question', 'segmented', 'equation', 'tag']
			for _, record in df.iterrows():
				yield str(_id), {'question':record['question'],'segmented':record['segmented'],'equation':record['equation'],'tag':record['tag']}
				_id += 1 

