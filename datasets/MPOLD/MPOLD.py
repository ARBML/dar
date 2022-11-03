import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import json

class MPOLD(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Id':datasets.Value('string'),'Platform':datasets.Value('string'),'Comment':datasets.Value('string'),'Majority_Label':datasets.Value('string'),'Agreement':datasets.Value('string'),'NumOfJudgementUsed':datasets.Value('string'),'Total_Judgement':datasets.Value('string'),'Vulgar:V/HateSpeech:HS/None:-':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/shammur/Arabic-Offensive-Multi-Platform-SocialMedia-Comment-Dataset/master/data/Arabic_offensive_comment_detection_annotation_4000_selected.xlsx']
		downloaded_files = dl_manager.download(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 8:
				continue
			df.columns = ['Id', 'Platform', 'Comment', 'Majority_Label', 'Agreement', 'NumOfJudgementUsed', 'Total_Judgement', 'Vulgar:V/HateSpeech:HS/None:-']
			for _, record in df.iterrows():
				yield str(_id), {'Id':record['Id'],'Platform':record['Platform'],'Comment':record['Comment'],'Majority_Label':record['Majority_Label'],'Agreement':record['Agreement'],'NumOfJudgementUsed':record['NumOfJudgementUsed'],'Total_Judgement':record['Total_Judgement'],'Vulgar:V/HateSpeech:HS/None:-':record['Vulgar:V/HateSpeech:HS/None:-']}
				_id += 1 

