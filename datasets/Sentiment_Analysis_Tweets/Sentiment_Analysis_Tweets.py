import os
import pandas as pd 
import datasets

class Sentiment_Analysis_Tweets(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Tweet':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Negative', 'Positive'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/motazsaad/arabic-sentiment-analysis/master/arabic_tweets_txt/negative_tweets_arabic_20181206_1k.txt', 'https://raw.githubusercontent.com/motazsaad/arabic-sentiment-analysis/master/arabic_tweets_txt/positive_tweets_arabic_20181206_4k.txt']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['Negative', 'Positive']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Tweet']
			for _, record in df.iterrows():
				yield str(_id), {'Tweet':record['Tweet'],'label':str(labels[i])}
				_id += 1 

