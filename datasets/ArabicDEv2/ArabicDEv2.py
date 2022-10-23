import os
import pandas as pd 
import datasets

class ArabicDEv2(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'query':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://zenodo.org/record/4560653/files/ArabicDEv2.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],'ArabicDEv2/ArabicDEv2.tsv')]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['id', 'query']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'query':record['query']}
				_id += 1 

