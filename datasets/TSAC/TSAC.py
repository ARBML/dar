import os
import pandas as pd 
import datasets

class TSAC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Negative', 'Positive'])}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/fbougares/TSAC/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['TSAC-master/train_neg.txt', 'TSAC-master/train_pos.txt']]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['TSAC-master/test_neg.txt', 'TSAC-master/test_pos.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['Negative', 'Positive']
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None)
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text'],'label':str(labels[i])}
				_id += 1 

