import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class nsurl_2019_task8_test(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'QuestionPairID':datasets.Value('string'),'question1':datasets.Value('string'),'question2':datasets.Value('string'),'label':datasets.Value('string')}))

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
		url = [os.path.abspath(os.path.expanduser(dl_manager.manual_dir))]
		downloaded_files = url
		return [datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'test.tsv'),]} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python', encoding = 'utf-8')
			if len(df.columns) != 4:
				continue
			df = df[['QuestionPairID', 'question1', 'question2', 'label']]
			for _, record in df.iterrows():
				yield str(_id), {'QuestionPairID':record['QuestionPairID'],'question1':record['question1'],'question2':record['question2'],'label':record['label']}
				_id += 1 

