import os
import pandas as pd 
import datasets

class Arabic_Hate_Speech(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'tweet':datasets.Value('string'),'is_off':datasets.Value('string'),'is_hate':datasets.Value('string'),'is_vlg':datasets.Value('string'),'is_vio':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://alt.qcri.org/resources1/OSACT2022/OSACT2022-sharedTask-dev.txt', 'https://alt.qcri.org/resources1/OSACT2022/OSACT2022-sharedTask-train.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[1]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['id', 'tweet', 'is_off', 'is_hate', 'is_vlg', 'is_vio']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'tweet':record['tweet'],'is_off':record['is_off'],'is_hate':record['is_hate'],'is_vlg':record['is_vlg'],'is_vio':record['is_vio']}
				_id += 1 

