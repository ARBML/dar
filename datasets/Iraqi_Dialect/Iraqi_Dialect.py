import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'No.':datasets.Value('string'),' Tex':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['False', 'IDK', 'N', 'True'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/ebady/Iraqi-Arabic-Dialect-Dataset/master/Data/Annotated_Tweets_Before_Preprocessing.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ';', skiprows = 0, error_bad_lines = False)
			df.columns = ['No.', ' Tex', 'Result']
			for _, record in df.iterrows():
				yield str(_id), {'No.':record['No.'],' Tex':record[' Tex'],'label':str(record['Result'])}
				_id += 1 

