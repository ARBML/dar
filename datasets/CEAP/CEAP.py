import os
import pandas as pd 
import datasets

class CEAP(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://sourceforge.net/projects/ceap-bp/files/CEAP.rar']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['t27.txt', 't10.txt', 't38.txt', 't26.txt', 't02.txt', 't28.txt', 't20.txt', 't15.txt', 't34.txt', 't01.txt', 't09.txt', 't36.txt', 't13.txt', 't14.txt', 't33.txt', 't08.txt', 't19.txt', 't06.txt', 't35.txt', 't21.txt', 't30.txt', 't07.txt', 't32.txt', 't04.txt', 't18.txt', 't17.txt', 't31.txt', 't37.txt', 't03.txt', 't11.txt', 't29.txt', 't23.txt', 't12.txt', 't24.txt', 't39.txt', 't16.txt', 't05.txt', 't25.txt', 't40.txt', 't22.txt']]})]

	def read_txt(self, filepath, skiprows = 0):
		lines = open(filepath, 'r').read().splitlines()[skiprows:]
		return pd.DataFrame(lines)
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = self.read_txt(filepath, skiprows = 0)
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text']}
				_id += 1 

