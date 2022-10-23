import os
import pandas as pd 
import datasets

class tweets_infectious_diseases(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'id':datasets.Value('string'),'coder 1_Label':datasets.Value('string'),'Coder 2_Label':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ["https://www.research.lancs.ac.uk/portal/files/267829955/Tweets_ID_with_Labels.rar'"]
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':[os.path.join(downloaded_files[0],f) for f in ['Tweets_ID_with_Labels/Tweets_ID_with_Labels.csv']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = ';', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['id', 'coder 1_Label', 'Coder 2_Label']
			for _, record in df.iterrows():
				yield str(_id), {'id':record['id'],'coder 1_Label':record['coder 1_Label'],'Coder 2_Label':record['Coder 2_Label']}
				_id += 1 

