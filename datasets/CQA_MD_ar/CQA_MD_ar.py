import os
import pandas as pd 
import datasets
from glob import glob
import zipfile
import re
from bs4 import BeautifulSoup
class CQA_MD_ar(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'QAquestion':datasets.Value('string'),'QAanswer':datasets.Value('string')}))

	def extract_all(self, dir):
		zip_files = glob(dir+'/**/**.zip', recursive=True)
		for file in zip_files:
			with zipfile.ZipFile(file) as item:
				item.extractall('/'.join(file.split('/')[:-1])) 


	def get_all_files(self, dir):
		files = []
		valid_file_ext = ['txt', 'csv', 'tsv', 'xlsx', 'xls', 'xml', 'json', 'jsonl', 'html', 'arff'] 
		for ext in valid_file_ext:
			files += glob(f"{dir}/**/**.{ext}", recursive = True)
		return files

	def _split_generators(self, dl_manager):
		url = ['http://alt.qcri.org/semeval2016/task3/data/uploads/semeval2016-task3-cqa-arabic-md-train-v1.3.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['SemEval2016-Task3-CQA-Arabic-MD-train-v1.3/SemEval2016-Task3-CQA-MD-dev.xml', 'SemEval2016-Task3-CQA-Arabic-MD-train-v1.3/SemEval2016-Task3-CQA-MD-train.xml']]}),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['SemEval2016-Task3-CQA-Arabic-MD-train-v1.3/SemEval2016-Task3-CQA-MD-dev.xml']]})]



	def get_data(self, bs, column):
		elements =  [attr[column] for attr in bs.find_all(attrs={column : re.compile(".")})]
		if len(elements) == 0:
			elements = [el.text for el in bs.find_all(column)]
		return elements

	def read_xml(self, path, columns):
		with open(path, 'rb') as f:
			data = f.read()
    
		bs = BeautifulSoup(data, "xml")
		data = {}
		for column in columns:
			elements = self.get_data(bs, column)
			data[column] = elements
		return pd.DataFrame(data)
	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = self.read_xml(filepath, ['QAquestion', 'QAanswer'])
			if len(df.columns) != 2:
				continue
			df.columns = ['QAquestion', 'QAanswer']
			for _, record in df.iterrows():
				yield str(_id), {'QAquestion':record['QAquestion'],'QAanswer':record['QAanswer']}
				_id += 1 

