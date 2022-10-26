import os
import pandas as pd 
import datasets

class WDC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Word':datasets.Value('string'),'Entity':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/Maha-J-Althobaiti/Arabic_NER_Wiki-Corpus/main/WDC.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['WDC.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ' ', skiprows = 0, error_bad_lines = False, header = None, engine = 'python')
			df.columns = ['Word', 'Entity']
			for _, record in df.iterrows():
				yield str(_id), {'Word':record['Word'],'Entity':record['Entity']}
				_id += 1 

