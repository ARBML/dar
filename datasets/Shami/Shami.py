import os
import pandas as pd 
import datasets

class Shami(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Lebanees', 'Palestinian', 'Jordinian', 'Syiran'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/GU-CLASP/shami-corpus/master/Data/Lebanees.txt', 'https://raw.githubusercontent.com/GU-CLASP/shami-corpus/master/Data/Palestinian.txt', 'https://raw.githubusercontent.com/GU-CLASP/shami-corpus/master/Data/jordinian.txt', 'https://raw.githubusercontent.com/GU-CLASP/shami-corpus/master/Data/syrian.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['Lebanees', 'Palestinian', 'Jordinian', 'Syiran']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['text']
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'label':str(labels[i])}
				_id += 1 

