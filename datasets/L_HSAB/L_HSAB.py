import os
import pandas as pd 
import datasets

class L_HSAB(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Tweet':datasets.Value('string'),'label': datasets.features.ClassLabel(names=[None, 'abusive', 'hate', 'normal'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/Hala-Mulki/L-HSAB-First-Arabic-Levantine-HateSpeech-Dataset/master/Dataset/L-HSAB']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': downloaded_files})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0,encoding= 'unicode_escape')
			df.columns = ['Tweet', 'Class']
			for _, record in df.iterrows():
				yield str(_id), {'Tweet':record['Tweet'],'label':str(record['Class'])}
				_id += 1 

