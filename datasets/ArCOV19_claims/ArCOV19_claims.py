import os
import pandas as pd 
import datasets

class ArCOV19_claims(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ClaimID':datasets.Value('string'),'Claim':datasets.Value('string'),'Category':datasets.Value('string'),'ClaimSource':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[False, True])}))
	def _split_generators(self, dl_manager):
		url = ['https://gitlab.com/bigirqu/ArCOV-19/-/raw/master/ArCOV19-Rumors/claim_verification/claims']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = [False, True]
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['ClaimID', 'Claim', 'ClaimLabel', 'Category', 'ClaimSource']
			for _, record in df.iterrows():
				yield str(_id), {'ClaimID':record['ClaimID'],'Claim':record['Claim'],'Category':record['Category'],'ClaimSource':record['ClaimSource'],'label':str(record['ClaimLabel'])}
				_id += 1 

