import os
import pandas as pd 
import datasets

class k(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Word_Phrase':datasets.Value('string'),'POS':datasets.Value('string'),'Polarity':datasets.Value('string'),'Score':datasets.Value('string'),'Synonyms':datasets.Value('string'),'Phrase':datasets.Value('string'),'Strength_word':datasets.Value('string'),'Nigation_word':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/almoslmi/masc/master/MASC%20Corpus.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':[os.path.join(downloaded_files[0],f) for f in ['Arabic Senti-Lexicon Corpus/lexicon All.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['Word_Phrase', 'POS', 'Polarity', 'Score', 'Synonyms', 'Phrase', 'Strength_word', 'Nigation_word']
			for _, record in df.iterrows():
				yield str(_id), {'Word_Phrase':record['Word_Phrase'],'POS':record['POS'],'Polarity':record['Polarity'],'Score':record['Score'],'Synonyms':record['Synonyms'],'Phrase':record['Phrase'],'Strength_word':record['Strength_word'],'Nigation_word':record['Nigation_word']}
				_id += 1 

