import os
import pandas as pd 
import datasets

class ArSenL(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Aramorph_lemma':datasets.Value('string'),'POS':datasets.Value('string'),'Positive_Sentiment_Score':datasets.Value('string'),'Negative_Sentiment_Score':datasets.Value('string'),'Confidence':datasets.Value('string'),'AWN_Offset':datasets.Value('string'),'SWN_Offset':datasets.Value('string'),'AWN_Lemma':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['http://oma-project.com/ArSenL/ArSenL.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['ArSenL_v1.0A.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ';', skiprows = 17, error_bad_lines = False, header = None, engine = 'python')
			df.columns = ['Aramorph_lemma', 'POS', 'Positive_Sentiment_Score', 'Negative_Sentiment_Score', 'Confidence', 'AWN_Offset', 'SWN_Offset', 'AWN_Lemma']
			for _, record in df.iterrows():
				yield str(_id), {'Aramorph_lemma':record['Aramorph_lemma'],'POS':record['POS'],'Positive_Sentiment_Score':record['Positive_Sentiment_Score'],'Negative_Sentiment_Score':record['Negative_Sentiment_Score'],'Confidence':record['Confidence'],'AWN_Offset':record['AWN_Offset'],'SWN_Offset':record['SWN_Offset'],'AWN_Lemma':record['AWN_Lemma']}
				_id += 1 

