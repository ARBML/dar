import os
import pandas as pd 
import datasets

class Habibi(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ï»¿songID':datasets.Value('string'),'Singer':datasets.Value('string'),'SongTitle':datasets.Value('string'),'SongWriter':datasets.Value('string'),'Composer':datasets.Value('string'),'LyricsOrder':datasets.Value('string'),'Lyrics':datasets.Value('string'),'SingerNationality':datasets.Value('string'),'SongDialect':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['http://ucrel-web.lancaster.ac.uk/habibi/habibi_full_corpus_csv.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in os.listdir(downloaded_files[0])]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '', skiprows = 0, error_bad_lines = False, header = 0,encoding= 'unicode_escape')
			df.columns = ['ï»¿songID', 'Singer', 'SongTitle', 'SongWriter', 'Composer', 'LyricsOrder', 'Lyrics', 'SingerNationality', 'SongDialect']
			for _, record in df.iterrows():
				yield str(_id), {'ï»¿songID':record['ï»¿songID'],'Singer':record['Singer'],'SongTitle':record['SongTitle'],'SongWriter':record['SongWriter'],'Composer':record['Composer'],'LyricsOrder':record['LyricsOrder'],'Lyrics':record['Lyrics'],'SingerNationality':record['SingerNationality'],'SongDialect':record['SongDialect']}
				_id += 1 

