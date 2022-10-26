import os
import pandas as pd 
import datasets

class AQMAR(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Word':datasets.Value('string'),'Entity':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['I-MIS1', 'B-MIS1', 'B-LOC', 'B-ORG', 'B-PER', 'I-MIS2', 'I-PER', 'I-ORG', 'O', 'B-MIS2', 'B-MIS0', 'I-MIS0', 'I-LOC','B-MIS3', 'I-MIS3', 'B-MIS', 'B-ENGLISH', 'I--ORG', 'B-MIS-1', 'IO', 'B-MIS-2', 'B-SPANISH', 'B-MISS', 'B-MISS1', 'OO', 'B-MIS1`', 'I-MIS'])}))
	def _split_generators(self, dl_manager):
		url = ['https://www.cs.cmu.edu/~ark/ArabicNER/AQMAR_Arabic_NER_corpus-1.0.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Raul_Gonzales.txt', 'Nuclear_Power.txt', 'Computer.txt', 'Portugal_football_team.txt', 'Christiano_Ronaldo.txt', 'Computer_Software.txt', 'Solaris.txt', 'Periodic_Table.txt', 'Enrico_Fermi.txt', 'Internet.txt', 'Richard_Stallman.txt', 'Linux.txt', 'Imam_Hussein_Shrine.txt', 'Crusades.txt', 'Damascus.txt', 'X_window_system.txt', 'Soccer_Worldcup.txt', 'Real_Madrid.txt', 'Razi.txt', 'Light.txt', 'Football.txt', 'Islamic_Golden_Age.txt', 'Ummaya_Mosque.txt', 'Islamic_History.txt', 'Ibn_Tolun_Mosque.txt', 'Summer_Olympics2004.txt', 'Physics.txt', 'Atom.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['I-MIS1', 'B-MIS1', 'B-LOC', 'B-ORG', 'B-PER', 'I-MIS2', 'I-PER', 'I-ORG', 'O', 'B-MIS2']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ' ', skiprows = 0, error_bad_lines = False, header = None, engine = 'python')
			df.columns = ['Word', 'Entity']
			for _, record in df.iterrows():
				yield str(_id), {'Word':record['Word'],'Entity':record['Entity'],'label':str(record['Entity'])}
				_id += 1 

