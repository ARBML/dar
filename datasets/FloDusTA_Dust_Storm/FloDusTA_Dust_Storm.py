import os
import pandas as pd 
import datasets

class FloDusTA_Dust_Storm(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id_str':datasets.Value('string'),'Unnamed: 1':datasets.Value('string'),'Unnamed: 2':datasets.Value('string'),'Unnamed: 3':datasets.Value('string'),'Annot1':datasets.Value('string'),'Annot2':datasets.Value('string'),'Annot3':datasets.Value('string'),'Unnamed: 7':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['IMFL', 'IMAC', 'IRNE', 'IMDS', 'EXCLUDE'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/BatoolHamawi/FloDusTA/master/FloDusTA-Dust-Storm-Collection.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 1, error_bad_lines = False, header = 0)
			df.columns = ['id_str', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Annot1', 'Annot2', 'Annot3', 'Unnamed: 7', 'Majority Labels']
			for _, record in df.iterrows():
				yield str(_id), {'id_str':record['id_str'],'Unnamed: 1':record['Unnamed: 1'],'Unnamed: 2':record['Unnamed: 2'],'Unnamed: 3':record['Unnamed: 3'],'Annot1':record['Annot1'],'Annot2':record['Annot2'],'Annot3':record['Annot3'],'Unnamed: 7':record['Unnamed: 7'],'label':str(record['Majority Labels'])}
				_id += 1 

