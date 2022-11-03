import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class emoji_sentiment_lexicon(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Unnamed: 0':datasets.Value('string'),'Emoji_ID':datasets.Value('string'),'Emoji':datasets.Value('string'),'Unicode_Name':datasets.Value('string'),'Arabic_Name':datasets.Value('string'),'Class':datasets.Value('string'),'Total_Occurrence':datasets.Value('string'),'Negativity_Occurrence_N(Negative)':datasets.Value('string'),'Neutrality_Occurrence_N(Neutral)':datasets.Value('string'),'Positivity_Occurrence_N(Positive)':datasets.Value('string'),'Negativite_Probability_P(Negative)':datasets.Value('string'),'Neutral_Probability_P(Neutral)':datasets.Value('string'),'Positive_Probability_P(Positive)':datasets.Value('string'),'Sentiment_Score_S':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['negative', 'neutral', 'positive'])}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff', 'wav', 'mp3']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://raw.githubusercontent.com/ShathaHakami/Arabic-Emoji-Sentiment-Lexicon-Version-1.0/main/Arabic_Emoji_Sentiment_Lexicon_Version_1.0.csv']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 15:
				continue
			df.columns = ['Unnamed: 0', 'Emoji_ID', 'Emoji', 'Unicode_Name', 'Arabic_Name', 'Class', 'Total_Occurrence', 'Negativity_Occurrence_N(Negative)', 'Neutrality_Occurrence_N(Neutral)', 'Positivity_Occurrence_N(Positive)', 'Negativite_Probability_P(Negative)', 'Neutral_Probability_P(Neutral)', 'Positive_Probability_P(Positive)', 'Sentiment_Score_S', 'Sentiment_Label_L']
			for _, record in df.iterrows():
				yield str(_id), {'Unnamed: 0':record['Unnamed: 0'],'Emoji_ID':record['Emoji_ID'],'Emoji':record['Emoji'],'Unicode_Name':record['Unicode_Name'],'Arabic_Name':record['Arabic_Name'],'Class':record['Class'],'Total_Occurrence':record['Total_Occurrence'],'Negativity_Occurrence_N(Negative)':record['Negativity_Occurrence_N(Negative)'],'Neutrality_Occurrence_N(Neutral)':record['Neutrality_Occurrence_N(Neutral)'],'Positivity_Occurrence_N(Positive)':record['Positivity_Occurrence_N(Positive)'],'Negativite_Probability_P(Negative)':record['Negativite_Probability_P(Negative)'],'Neutral_Probability_P(Neutral)':record['Neutral_Probability_P(Neutral)'],'Positive_Probability_P(Positive)':record['Positive_Probability_P(Positive)'],'Sentiment_Score_S':record['Sentiment_Score_S'],'label':str(record['Sentiment_Label_L'])}
				_id += 1 

