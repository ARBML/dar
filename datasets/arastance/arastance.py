import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'filename':datasets.Value('string'),'claim':datasets.Value('string'),'claim_url':datasets.Value('string'),'article':datasets.Value('string'),'stance':datasets.features.ClassLabel(names=['Discuss', 'Disagree', 'Unrelated', 'Agree']),'article_title':datasets.Value('string'),'article_url':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/Tariq60/arastance/main/data/dev.jsonl', 'https://raw.githubusercontent.com/Tariq60/arastance/main/data/test.jsonl', 'https://raw.githubusercontent.com/Tariq60/arastance/main/data/train.jsonl']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[1]]}),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[2]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_json(open(filepath, 'rb'), lines=True)
			df.columns = ['filename', 'claim', 'claim_url', 'article', 'stance', 'article_title', 'article_url']
			for _, record in df.iterrows():
				for i in range(len(record['article'])):
					yield str(_id), {'filename':record['filename'],'claim':record['claim'],'claim_url':record['claim_url'],'article':record['article'][i],'stance':record['stance'][i],'article_title':record['article_title'][i],'article_url':record['article_url'][i]}
					_id += 1 

