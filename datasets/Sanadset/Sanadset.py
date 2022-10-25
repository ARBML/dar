import os
import pandas as pd 
import datasets

class Sanadset(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Hadith':datasets.Value('string'),'Book':datasets.Value('string'),'Num_hadith':datasets.Value('string'),'Matn':datasets.Value('string'),'Sanad':datasets.Value('string'),'Sanad_Length':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/5xth87zwb5-4.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Sanadset 650K Data on Hadith Narrators/sanadset.csv']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['Hadith', 'Book', 'Num_hadith', 'Matn', 'Sanad', 'Sanad_Length']
			for _, record in df.iterrows():
				yield str(_id), {'Hadith':record['Hadith'],'Book':record['Book'],'Num_hadith':record['Num_hadith'],'Matn':record['Matn'],'Sanad':record['Sanad'],'Sanad_Length':record['Sanad_Length']}
				_id += 1 

