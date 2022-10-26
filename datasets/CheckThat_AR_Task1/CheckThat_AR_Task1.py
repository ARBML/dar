import os
import pandas as pd 
import datasets

class CheckThat_AR_Task1(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'task':datasets.Value('string'),'id':datasets.Value('string'),'label':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0, 1])}))
	def _split_generators(self, dl_manager):
		url = ['https://gitlab.com/bigirqu/checkthat-ar/-/raw/master/data/2020/task1/training/CT20-AR-Train-T1-Labels.txt', 'https://gitlab.com/bigirqu/checkthat-ar/-/raw/master/data/2020/task1/testing/CT20-AR-Test-T1-Labels.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[1]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = [0, 1]
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = None, engine = 'python')
			df.columns = ['task', 'id', 'label']
			for _, record in df.iterrows():
				yield str(_id), {'task':record['task'],'id':record['id'],'label':str(record['label'])}
				_id += 1 

