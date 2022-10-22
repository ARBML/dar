import os
import pandas as pd 
import datasets
class AjgtTwitterAr(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'SS':datasets.Value('string'),'SV':datasets.Value('string'),'Verse1':datasets.Value('string'),'TS':datasets.Value('string'),'TV':datasets.Value('string'),'Verse2':datasets.Value('string'),'Label':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/ShathaTm/Quran_Hadith_Datasets/main/QQ_Ar_Tafseer_training_8144.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[0]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False)
			df.columns = ['SS', 'SV', 'Verse1', 'TS', 'TV', 'Verse2', 'Label']
			for _, record in df.iterrows():
				yield str(_id), {'SS':record['SS'],'SV':record['SV'],'Verse1':record['Verse1'],'TS':record['TS'],'TV':record['TV'],'Verse2':record['Verse2'],'Label':record['Label']}
				_id += 1 

