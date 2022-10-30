import os
import pandas as pd 
import datasets
from glob import glob

class MASC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ID':datasets.Value('string'),'Domain':datasets.Value('string'),'Product':datasets.Value('string'),'Country':datasets.Value('string'),'Text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['negative', 'Positive', 'Negative'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/almoslmi/masc/master/MASC%20Corpus.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(downloaded_files[0]+'/MASC Corpus/Excel Format/MASC corpus - Main.xlsx')})]
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			df.columns = ['ID', 'Domain', 'Product', 'Country', 'Polarity', 'Text']
			for _, record in df.iterrows():
				yield str(_id), {'ID':record['ID'],'Domain':record['Domain'],'Product':record['Product'],'Country':record['Country'],'Text':record['Text'],'label':str(record['Polarity'])}
				_id += 1 

