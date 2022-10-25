import os
import pandas as pd 
import datasets

class COVID_19_Disinformation_ar(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet_id':datasets.Value('string'),'q1_label':datasets.Value('string'),'q2_label':datasets.Value('string'),'q3_label':datasets.Value('string'),'q4_label':datasets.Value('string'),'q5_label':datasets.Value('string'),'q6_label':datasets.Value('string'),'q7_label':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/firojalam/COVID-19-disinformation/master/data/arabic/covid19_disinfo_arabic_multiclass_train.tsv', 'https://raw.githubusercontent.com/firojalam/COVID-19-disinformation/master/data/arabic/covid19_disinfo_arabic_multiclass_dev.tsv', 'https://raw.githubusercontent.com/firojalam/COVID-19-disinformation/master/data/arabic/covid19_disinfo_arabic_multiclass_test.tsv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [downloaded_files[0]]}),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [downloaded_files[1]]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [downloaded_files[2]]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['tweet_id', 'q1_label', 'q2_label', 'q3_label', 'q4_label', 'q5_label', 'q6_label', 'q7_label']
			for _, record in df.iterrows():
				yield str(_id), {'tweet_id':record['tweet_id'],'q1_label':record['q1_label'],'q2_label':record['q2_label'],'q3_label':record['q3_label'],'q4_label':record['q4_label'],'q5_label':record['q5_label'],'q6_label':record['q6_label'],'q7_label':record['q7_label']}
				_id += 1 

