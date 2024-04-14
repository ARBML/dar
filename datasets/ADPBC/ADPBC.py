import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ADPBC(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Id':datasets.Value('string'),'Form':datasets.Value('string'),'Lemma':datasets.Value('string'),'UPosTag':datasets.Value('string'),'XPosTag':datasets.Value('string'),'Feats':datasets.Value('string'),'Head':datasets.Value('string'),'DepRel':datasets.Value('string'),'Deps':datasets.Value('string'),'Misc':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['DB_6_REL', 'TEXT_12_SPORT_HEALTH', 'DB_4_BIOMIDICAL', 'TEXT_15_WEATHER', 'TEXT_16_ECNOMIC', 'TEXT_13_SPORT&ECNOMIC', 'DB_3_SPORT', 'DB_9_SOCIAL', 'DB_10_SOCILA&ECNOMIC', 'DB_2_WEATHER', 'DB_11_ECNOMIC&SOCIAL', 'TEXT_14_REL&HEALTH', 'DB_1_News', 'DB_8_SPORT', 'DB_7_REL', 'DB_5_NEWS'])}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'wav', 'mp3', 'jpg', 'png']
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['https://github.com/salsama/Arabic-Information-Extraction-Corpus/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/Arabic-Information-Extraction-Corpus-master/**.xlsx')),} })]


	def get_label_from_path(self, labels, label):
		for l in labels:
			if l == label:
				return label

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 10:
				continue
			df = df[['Id', 'Form', 'Lemma', 'UPosTag', 'XPosTag', 'Feats', 'Head', 'DepRel', 'Deps', 'Misc']]
			label = self.get_label_from_path(['DB_6_REL', 'TEXT_12_SPORT_HEALTH', 'DB_4_BIOMIDICAL', 'TEXT_15_WEATHER', 'TEXT_16_ECNOMIC', 'TEXT_13_SPORT&ECNOMIC', 'DB_3_SPORT', 'DB_9_SOCIAL', 'DB_10_SOCILA&ECNOMIC', 'DB_2_WEATHER', 'DB_11_ECNOMIC&SOCIAL', 'TEXT_14_REL&HEALTH', 'DB_1_News', 'DB_8_SPORT', 'DB_7_REL', 'DB_5_NEWS'], filepath.split('/')[-1].split('.')[-2])
			for _, record in df.iterrows():
				yield str(_id), {'Id':record['Id'],'Form':record['Form'],'Lemma':record['Lemma'],'UPosTag':record['UPosTag'],'XPosTag':record['XPosTag'],'Feats':record['Feats'],'Head':record['Head'],'DepRel':record['DepRel'],'Deps':record['Deps'],'Misc':record['Misc'],'label':str(label)}
				_id += 1 

