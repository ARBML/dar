import os
import pandas as pd 
import datasets

class SemEval_2017_ar_subtask_BD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Id':datasets.Value('string'),'category':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['positive', 'negative'])}))
	def _split_generators(self, dl_manager):
		url = ['http://alt.qcri.org/semeval2017/task4/data/uploads/train-dev-splits-arabic.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['DOWNLOAD/TRAIN-ONLY/SemEval2017-task4-train-only.subtask-BD.arabic.txt']]}),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['DOWNLOAD/DEV/SemEval2017-task4-dev.subtask-BD.arabic.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['positive', 'negative']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Id', 'category', 'Sentiment']
			for _, record in df.iterrows():
				yield str(_id), {'Id':record['Id'],'category':record['category'],'label':str(record['Sentiment'])}
				_id += 1 

