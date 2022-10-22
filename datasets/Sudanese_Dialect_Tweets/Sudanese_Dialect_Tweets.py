import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Tweet':datasets.Value('string'),'Annotator 1':datasets.Value('string'),'Annotator 2':datasets.Value('string'),'Annotator 3':datasets.Value('string'),'Date':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0.0, 1.0, nan, nan, nan, nan, nan, nan, nan, nan, -1.0])}))
	def _split_generators(self, dl_manager):
		url = ['https://docs.google.com/spreadsheets/d/1bNwimEQFMWtjlsKtL8PH_RNFNjg-b6p3/gviz/tq?tqx=out:csv&sheet=Sheet1']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False)
			df.columns = ['Tweet', 'Annotator 1', 'Annotator 2', 'Annotator 3', 'Mode', 'Date']
			for _, record in df.iterrows():
				yield str(_id), {'Tweet':record['Tweet'],'Annotator 1':record['Annotator 1'],'Annotator 2':record['Annotator 2'],'Annotator 3':record['Annotator 3'],'Date':record['Date'],'label':str(record['Mode'])}
				_id += 1 

