import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'first_sentence':datasets.Value('string'),'second_sentence':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[0, 1])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/msmadi/Arabic-Dataset-for-Commonsense-Validationion/master/Data.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],'Dev.txt')]}),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],'Train.txt')]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False)
			df.columns = ['id', 'first_sentence', 'second_sentence', 'label']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'first_sentence':record['first_sentence'],'second_sentence':record['second_sentence'],'label':str(record['label'])}
				_id += 1 

