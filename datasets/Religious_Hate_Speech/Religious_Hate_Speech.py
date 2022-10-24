import os
import pandas as pd 
import datasets

class Religious_Hate_Speech(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'hate':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/nuhaalbadi/Arabic_hatespeech/master/train.csv', 'https://raw.githubusercontent.com/nuhaalbadi/Arabic_hatespeech/master/test.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[1]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['id', 'hate']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'hate':record['hate']}
				_id += 1 

