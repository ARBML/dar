import os
import pandas as pd 
import datasets

class AraFacts(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ClaimID':datasets.Value('string'),'claim':datasets.Value('string'),'description':datasets.Value('string'),'source':datasets.Value('string'),'date':datasets.Value('string'),'source_label':datasets.Value('string'),'normalized_label':datasets.Value('string'),'source_category':datasets.Value('string'),'normalized_category':datasets.Value('string'),'source_url':datasets.Value('string'),'claim_urls':datasets.Value('string'),'evidence_urls':datasets.Value('string'),'claim_type':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://gitlab.com/bigirqu/AraFacts/-/raw/master/Dataset/AraFacts.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0,encoding= 'unicode_escape')
			df.columns = ['ClaimID', 'claim', 'description', 'source', 'date', 'source_label', 'normalized_label', 'source_category', 'normalized_category', 'source_url', 'claim_urls', 'evidence_urls', 'claim_type']
			for _, record in df.iterrows():
				yield str(_id), {'ClaimID':record['ClaimID'],'claim':record['claim'],'description':record['description'],'source':record['source'],'date':record['date'],'source_label':record['source_label'],'normalized_label':record['normalized_label'],'source_category':record['source_category'],'normalized_category':record['normalized_category'],'source_url':record['source_url'],'claim_urls':record['claim_urls'],'evidence_urls':record['evidence_urls'],'claim_type':record['claim_type']}
				_id += 1 

