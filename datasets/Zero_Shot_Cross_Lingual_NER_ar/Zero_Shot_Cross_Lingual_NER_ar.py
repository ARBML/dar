import os
import pandas as pd 
import datasets

class Zero_Shot_Cross_Lingual_NER_ar(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Word':datasets.Value('string'),'Entity':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['B-ORG', 'I-LOC', 'I-PER', 'B-LOC', 'I-MISC', 'I-ORG', 'B-MISC', 'B-PER', 'O'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/ntunlp/Zero-Shot-Cross-Lingual-NER/master/ner_data/ar/ar.train', 'https://raw.githubusercontent.com/ntunlp/Zero-Shot-Cross-Lingual-NER/master/ner_data/ar/ar.testa', 'https://raw.githubusercontent.com/ntunlp/Zero-Shot-Cross-Lingual-NER/master/ner_data/ar/ar.testb']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[1],downloaded_files[2]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['B-ORG', 'I-LOC', 'I-PER', 'B-LOC', 'I-MISC', 'I-ORG', 'B-MISC', 'B-PER', 'O']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ' ', skiprows = 0, error_bad_lines = False, header = None, engine = 'python')
			df.columns = ['Word', 'Entity']
			for _, record in df.iterrows():
				yield str(_id), {'Word':record['Word'],'Entity':record['Entity'],'label':str(record['Entity'])}
				_id += 1 

