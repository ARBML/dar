import os
import pandas as pd 
import datasets

class SADID(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/we7el/SADID/main/sadid-arabic-dialect-benchmark-dataset.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['sadid-arabic-dialect-benchmark-dataset/data/dev-levantine.txt', 'sadid-arabic-dialect-benchmark-dataset/data/dev-egyptian.txt', 'sadid-arabic-dialect-benchmark-dataset/data/testdev-english.txt', 'sadid-arabic-dialect-benchmark-dataset/data/dev-english.txt', 'sadid-arabic-dialect-benchmark-dataset/data/testdev-egyptian.txt', 'sadid-arabic-dialect-benchmark-dataset/data/testdev-levantine.txt']]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['sadid-arabic-dialect-benchmark-dataset/data/test-msa.txt', 'sadid-arabic-dialect-benchmark-dataset/data/testdev-english.txt', 'sadid-arabic-dialect-benchmark-dataset/data/test-levantine.txt', 'sadid-arabic-dialect-benchmark-dataset/data/testdev-egyptian.txt', 'sadid-arabic-dialect-benchmark-dataset/data/test-english.txt', 'sadid-arabic-dialect-benchmark-dataset/data/testdev-levantine.txt', 'sadid-arabic-dialect-benchmark-dataset/data/test-egyptian.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			print(filepath)
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text']}
				_id += 1 

