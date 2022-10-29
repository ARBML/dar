import os
import pandas as pd 
import datasets

class kawarith_Kuwait_floods_18(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ID':datasets.Value('string'),'Information type':datasets.Value('string'),'affected_individuals_and_help':datasets.Value('string'),'caution_advice_and_crisis_updates':datasets.Value('string'),'emotional_support_and_prayers':datasets.Value('string'),'infrastructure_and_utilities_damage':datasets.Value('string'),'irrelevant':datasets.Value('string'),'opinions_and_criticism':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/alaa-a-a/kawarith/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['kawarith-main/Labelled Data/Kuwait_floods-18/Kuwait_train.csv']]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['kawarith-main/Labelled Data/Kuwait_floods-18/Kuwait_test.csv']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['ID', 'Information type', 'affected_individuals_and_help', 'caution_advice_and_crisis_updates', 'emotional_support_and_prayers', 'infrastructure_and_utilities_damage', 'irrelevant', 'opinions_and_criticism']
			for _, record in df.iterrows():
				yield str(_id), {'ID':record['ID'],'Information type':record['Information type'],'affected_individuals_and_help':record['affected_individuals_and_help'],'caution_advice_and_crisis_updates':record['caution_advice_and_crisis_updates'],'emotional_support_and_prayers':record['emotional_support_and_prayers'],'infrastructure_and_utilities_damage':record['infrastructure_and_utilities_damage'],'irrelevant':record['irrelevant'],'opinions_and_criticism':record['opinions_and_criticism']}
				_id += 1 

