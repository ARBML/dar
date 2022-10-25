import os
import pandas as pd 
import datasets

class Named_Entities_Lexicon(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Org', 'Pers', 'Loc'])}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/Hkiri-Emna/Named_Entities_Lexicon_Project/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Named_Entities_Lexicon_Project-master/Org-En-Ar/Org.Ar.txt', 'Named_Entities_Lexicon_Project-master/Pers-En-Ar/Pers.Ar.txt', 'Named_Entities_Lexicon_Project-master/Loc-En-Ar/Loc.Ar.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['Org', 'Pers', 'Loc']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text'],'label':str(labels[i])}
				_id += 1 

