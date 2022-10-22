import os
import pandas as pd 
import datasets
class Sudanese_Dialect_Tweet_Tele(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Tweet ID':datasets.Value('string'),'Tweet Text':datasets.Value('string'),'Date':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['NEGATIVE', 'POSITIVE', 'OBJECTIVE'])}))
	def _split_generators(self, dl_manager):
		url = ['https://docs.google.com/spreadsheets/d/13fIV8oHss-QRBKN-2h5LYF1i_1O9qH1R/gviz/tq?tqx=out:csv&sheet=Sheet1']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False)
			df.columns = ['Tweet ID', 'Tweet Text', 'Classification', 'Date']
			for _, record in df.iterrows():
				if str(record['Classification']) == 'nan':
					continue
				yield str(_id), {'Tweet ID':record['Tweet ID'],'Tweet Text':record['Tweet Text'],'Date':record['Date'],'label':str(record['Classification'])}
				_id += 1 

