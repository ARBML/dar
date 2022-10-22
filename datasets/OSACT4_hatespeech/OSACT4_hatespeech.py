import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet':datasets.Value('string'),'offensive':datasets.Value('string'),'hate':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['http://edinburghnlp.inf.ed.ac.uk/workshops/OSACT4/datasets/OSACT2020-sharedTask-train.txt', 'http://edinburghnlp.inf.ed.ac.uk/workshops/OSACT4/datasets/OSACT2020-sharedTask-dev.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [downloaded_files[1]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False)
			df.columns = ['tweet', 'offensive', 'hate']
			for _, record in df.iterrows():
				yield str(_id), {'tweet':record['tweet'],'offensive':record['offensive'],'hate':record['hate']}
				_id += 1 

