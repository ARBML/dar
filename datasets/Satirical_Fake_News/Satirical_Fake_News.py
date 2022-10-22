import os
import pandas as pd 
import datasets
class Satirical_Fake_News(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/sadanyh/Arabic-Satirical-Fake-News-Dataset/master/fake_news.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in os.listdir(downloaded_files[0])]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text']}
				_id += 1 

