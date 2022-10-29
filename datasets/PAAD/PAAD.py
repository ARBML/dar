import os
import pandas as pd 
import datasets

class PAAD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'text':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Reform', 'Revolutionary', 'Conservative'])}))
	def _split_generators(self, dl_manager):
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/spvbf5bgjs-2.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Corpus Version/V1.xls']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = ['Reform', 'Revolutionary', 'Conservative']
		for i,filepath in enumerate(filepaths):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			df.columns = ['text', 'lable']
			for _, record in df.iterrows():
				yield str(_id), {'text':record['text'],'label':str(record['lable'])}
				_id += 1 

