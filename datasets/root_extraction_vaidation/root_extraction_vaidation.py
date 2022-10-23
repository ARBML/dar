import os
import pandas as pd 
import datasets

class root_extraction_vaidation(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'word':datasets.Value('string'),'root':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/arabic-digital-humanities/root-extraction-validation-data/master/gs/0450AbuHasanMawardi.HawiKabir-sample.csv', 'https://raw.githubusercontent.com/arabic-digital-humanities/root-extraction-validation-data/master/gs/0460ShaykhTusi.Mabsut-sample.csv', 'https://raw.githubusercontent.com/arabic-digital-humanities/root-extraction-validation-data/master/gs/0483IbnAhmadSarakhsi.Mabsut-sample.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['word', 'root']
			for _, record in df.iterrows():
				yield str(_id), {'word':record['word'],'root':record['root']}
				_id += 1 

