import os
import pandas as pd 
import datasets
class ArSAS(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'#Tweet_ID':datasets.Value('string'),'Tweet_text':datasets.Value('string'),'Topic':datasets.Value('string'),'Sentiment_label_confidence':datasets.Value('string'),'Speech_act_label':datasets.Value('string'),'Speech_act_label_confidence':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['Negative', 'Neutral', 'Positive', 'Mixed'])}))
	def _split_generators(self, dl_manager):
		url = ['https://homepages.inf.ed.ac.uk/wmagdy/Resources/ArSAS.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in os.listdir(downloaded_files[0])]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['#Tweet_ID', 'Tweet_text', 'Topic', 'Sentiment_label', 'Sentiment_label_confidence', 'Speech_act_label', 'Speech_act_label_confidence']
			for _, record in df.iterrows():
				yield str(_id), {'#Tweet_ID':record['#Tweet_ID'],'Tweet_text':record['Tweet_text'],'Topic':record['Topic'],'Sentiment_label_confidence':record['Sentiment_label_confidence'],'Speech_act_label':record['Speech_act_label'],'Speech_act_label_confidence':record['Speech_act_label_confidence'],'label':str(record['Sentiment_label'])}
				_id += 1 

