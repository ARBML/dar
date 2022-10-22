import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Unnamed: 0':datasets.Value('string'),'ID':datasets.Value('string'),'Category':datasets.Value('string'),'Source':datasets.Value('string'),'Title':datasets.Value('string'),'Subtitle':datasets.Value('string'),'Image':datasets.Value('string'),'Caption':datasets.Value('string'),'Text':datasets.Value('string'),'URL':datasets.Value('string'),'FullText':datasets.Value('string'),'FullTextCleaned':datasets.Value('string'),'FullTextWords':datasets.Value('string'),'WordsCounts':datasets.Value('string'),'Date':datasets.Value('string'),'Time':datasets.Value('string'),'Images':datasets.Value('string'),'Captions':datasets.Value('string'),'Terms':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/alioh/AlRiyadh-Newspaper-Covid-Dataset/master/Alriyadh_News_Dataset.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],'alriyadh_Published.csv')]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False)
			df.columns = ['Unnamed: 0', 'ID', 'Category', 'Source', 'Title', 'Subtitle', 'Image', 'Caption', 'Text', 'URL', 'FullText', 'FullTextCleaned', 'FullTextWords', 'WordsCounts', 'Date', 'Time', 'Images', 'Captions', 'Terms']
			for _, record in df.iterrows():
				yield str(_id), {'Unnamed: 0':record['Unnamed: 0'],'ID':record['ID'],'Category':record['Category'],'Source':record['Source'],'Title':record['Title'],'Subtitle':record['Subtitle'],'Image':record['Image'],'Caption':record['Caption'],'Text':record['Text'],'URL':record['URL'],'FullText':record['FullText'],'FullTextCleaned':record['FullTextCleaned'],'FullTextWords':record['FullTextWords'],'WordsCounts':record['WordsCounts'],'Date':record['Date'],'Time':record['Time'],'Images':record['Images'],'Captions':record['Captions'],'Terms':record['Terms']}
				_id += 1 

