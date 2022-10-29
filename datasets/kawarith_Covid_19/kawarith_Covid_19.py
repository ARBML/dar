import os
import pandas as pd 
import datasets

class kawarith_Covid_19(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ID':datasets.Value('string'),'Relevance':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/alaa-a-a/kawarith/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['kawarith-main/Labelled Data/Covid-19/Covid_test.csv']]}),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['kawarith-main/Labelled Data/Covid-19/Covid_train.csv']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['ID', 'Relevance']
			for _, record in df.iterrows():
				yield str(_id), {'ID':record['ID'],'Relevance':record['Relevance']}
				_id += 1 

