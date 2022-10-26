import os
import pandas as pd 
import datasets

class COVID_19_Arabic_Tweets_Dataset(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Id':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/SarahAlqurashi/COVID-19-Arabic-Tweets-Dataset/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-03.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-11.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-14.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-06.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-10.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-01.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-05.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-13.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-04.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-02.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-09.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-07.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-08.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-12.csv', 'COVID-19-Arabic-Tweets-Dataset-master/COVID19-tweetID-2020-04/COVID19-tweetID-2020-04-15.csv']]})]

	def read_txt(self, filepath, skiprows = 0):
		lines = open(filepath, 'r').read().splitlines()[skiprows:]
		return pd.DataFrame(lines)
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = self.read_txt(filepath, skiprows = 1)
			df.columns = ['Id']
			for _, record in df.iterrows():
				yield str(_id), {'Id':record['Id']}
				_id += 1 

