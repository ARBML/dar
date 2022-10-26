import os
import pandas as pd 
import datasets

class Arabic_News_Tweets(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Posted Date':datasets.Value('string'),'Tweet ID':datasets.Value('string'),'Label':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Sport News', 'Quality Life News', 'General News', 'Economic News', 'Regional News'])}))
	def _split_generators(self, dl_manager):
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/9dxgbgx86k-3.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Arabic_Tweets_News/Dataset_V02.xlsx']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['Sport News', 'Quality Life News', 'General News', 'Economic News', 'Regional News']
		for i,filepath in enumerate(filepaths):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			df.columns = ['Posted Date', 'Tweet ID', 'Label']
			for _, record in df.iterrows():
				yield str(_id), {'Posted Date':record['Posted Date'],'Tweet ID':record['Tweet ID'],'Label':record['Label'],'label':str(record['Label'])}
				_id += 1 

