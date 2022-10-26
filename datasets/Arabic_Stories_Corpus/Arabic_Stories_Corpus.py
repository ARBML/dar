import os
import pandas as pd 
import datasets
import re
from bs4 import BeautifulSoup
class Arabic_Stories_Corpus(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'title':datasets.Value('string'),'author':datasets.Value('string'),'publisher':datasets.Value('string'),'date':datasets.Value('string'),'pubPlace':datasets.Value('string'),'body':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/motazsaad/Arabic-Stories-Corpus/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Arabic-Stories-Corpus-master/cca-stories-2003/CHD09.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD22.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S12.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S25.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S04.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S14.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD03.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S15.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD20.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S20.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD24.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD23.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S19.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD06.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD21.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S11.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD25.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD16.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S17.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S10.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S27.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S08.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD11.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S05.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S09.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD01.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S16.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S22.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD07.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S26.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S13.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S31.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD14.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD13.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S03.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD04.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S01.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S07.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S02.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD08.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD19.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S23.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S30.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD15.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD26.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD17.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD18.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD12.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S18.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD10.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S29.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S28.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S06.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S24.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD05.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD02.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/CHD27.xml', 'Arabic-Stories-Corpus-master/cca-stories-2003/S21.xml']]})]


	def get_data(self, bs, column):
		elements =  [attr[column] for attr in bs.find_all(attrs={column : re.compile(".")})]
		if len(elements) == 0:
			elements = [el.text for el in bs.find_all(column)]
		return elements

	def read_xml(self, path, columns):
		with open(path, 'rb') as f:
			data = f.read()
    
		bs = BeautifulSoup(data, "xml")
		data = {}
		for column in columns:
			elements = self.get_data(bs, column)
			data[column] = elements
		return pd.DataFrame(data)
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = self.read_xml(filepath, ['title', 'author', 'publisher', 'date', 'pubPlace', 'body'])
			df.columns = ['title', 'author', 'publisher', 'date', 'pubPlace', 'body']
			for _, record in df.iterrows():
				yield str(_id), {'title':record['title'],'author':record['author'],'publisher':record['publisher'],'date':record['date'],'pubPlace':record['pubPlace'],'body':record['body']}
				_id += 1 

