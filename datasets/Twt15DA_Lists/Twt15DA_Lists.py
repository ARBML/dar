import os
import pandas as pd 
import datasets

class Twt15DA_Lists(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'word':datasets.Value('string'),'score':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['uae', 'iraq', 'egypt', 'libya', 'syria', 'qatar', 'morocco', 'jordan', 'saudi', 'bahrain', 'algeria', 'oman', 'kuwait', 'yemen', 'tunis'])}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/Maha-J-Althobaiti/Twt15DA_Lists/main/Twt15DA_Lists.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Twt15DA_Lists/list_uae_pmi.txt', 'Twt15DA_Lists/list_iraq_pmi.txt', 'Twt15DA_Lists/list_egypt_pmi.txt', 'Twt15DA_Lists/list_libya_pmi.txt', 'Twt15DA_Lists/list_syria_pmi.txt', 'Twt15DA_Lists/list_qatar_pmi.txt', 'Twt15DA_Lists/list_morocco_pmi.txt', 'Twt15DA_Lists/list_jordan_pmi.txt', 'Twt15DA_Lists/list_saudi_pmi.txt', 'Twt15DA_Lists/list_bahrain_pmi.txt', 'Twt15DA_Lists/list_algeria_pmi.txt', 'Twt15DA_Lists/list_oman_pmi.txt', 'Twt15DA_Lists/list_kuwait_pmi.txt', 'Twt15DA_Lists/list_yemen_pmi.txt', 'Twt15DA_Lists/list_tunis_pmi.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['uae', 'iraq', 'egypt', 'libya', 'syria', 'qatar', 'morocco', 'jordan', 'saudi', 'bahrain', 'algeria', 'oman', 'kuwait', 'yemen', 'tunis']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['word', 'score']
			for _, record in df.iterrows():
				yield str(_id), {'word':record['word'],'score':record['score'],'label':str(labels[i])}
				_id += 1 

