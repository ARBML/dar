import os
import pandas as pd 
import datasets

class MLMA_hate_speech_ar(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'HITId':datasets.Value('string'),'tweet':datasets.Value('string'),'sentiment':datasets.Value('string'),'directness':datasets.Value('string'),'annotator_sentiment':datasets.Value('string'),'target':datasets.Value('string'),'group':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/HKUST-KnowComp/MLMA_hate_speech/master/hate_speech_mlma.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['hate_speech_mlma/ar_dataset.csv']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			df.columns = ['HITId', 'tweet', 'sentiment', 'directness', 'annotator_sentiment', 'target', 'group']
			for _, record in df.iterrows():
				yield str(_id), {'HITId':record['HITId'],'tweet':record['tweet'],'sentiment':record['sentiment'],'directness':record['directness'],'annotator_sentiment':record['annotator_sentiment'],'target':record['target'],'group':record['group']}
				_id += 1 

