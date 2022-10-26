import os
import pandas as pd 
import datasets

class Arabic_News(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/motazsaad/Arabic-News/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['Arabic-News-master/corpora/arabic.euronews.com_20190409_001.txt', 'Arabic-News-master/corpora/aljazeera.net_20190419_006.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_008.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_000.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_001.txt', 'Arabic-News-master/corpora/arabic.cnn.com_20190419_000.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_012.txt', 'Arabic-News-master/corpora/aljazeera.net_20190419_000.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_003.txt', 'Arabic-News-master/corpora/aljazeera.net_20190419_004.txt', 'Arabic-News-master/corpora/aljazeera.net_20190419_001.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_007.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_018.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_006.txt', 'Arabic-News-master/corpora/bbc.com_20190409_001.txt', 'Arabic-News-master/corpora/aljazeera.net_20190419_005.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_009.txt', 'Arabic-News-master/corpora/aljazeera.net_20190419_002.txt', 'Arabic-News-master/corpora/bbc.com_20190409_005.txt', 'Arabic-News-master/corpora/bbc.com_20190409_003.txt', 'Arabic-News-master/corpora/bbc.com_20190409_000.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_015.txt', 'Arabic-News-master/corpora/aljazeera.net_20190419_003.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_013.txt', 'Arabic-News-master/corpora/bbc.com_20190409_004.txt', 'Arabic-News-master/corpora/bbc.com_20190409_002.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_005.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_004.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_017.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_014.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_016.txt', 'Arabic-News-master/corpora/arabic.cnn.com_20190419_001.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_010.txt', 'Arabic-News-master/corpora/arabic.euronews.com_20190409_002.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_011.txt', 'Arabic-News-master/corpora/arabic.euronews.com_20190409_000.txt', 'Arabic-News-master/corpora/arabic.rt.com_20190419_002.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None, engine = 'python')
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text']}
				_id += 1 

