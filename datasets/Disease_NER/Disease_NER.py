import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class Disease_NER(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Word i':datasets.Value('string'),'Word i POS':datasets.Value('string'),'Stopword':datasets.Value('string'),'Word i Gazetteers':datasets.Value('string'),'Word i Lexical marker':datasets.Value('string'),'Word i definiteness':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['I', 'O', 'B'])}))

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
		url = ['https://mdpi-res.com/d_attachment/data/data-05-00060/article_deploy/data-05-00060-s001.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': glob(downloaded_files[0]+'/first-annotator/IOB.csv')})]

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = 0)
			if len(df.columns) != 7:
				continue
			df.columns = ['Word i', 'Word i entity tag', 'Word i POS', 'Stopword', 'Word i Gazetteers', 'Word i Lexical marker', 'Word i definiteness']
			for _, record in df.iterrows():
				yield str(_id), {'Word i':record['Word i'],'Word i POS':record['Word i POS'],'Stopword':record['Stopword'],'Word i Gazetteers':record['Word i Gazetteers'],'Word i Lexical marker':record['Word i Lexical marker'],'Word i definiteness':record['Word i definiteness'],'label':str(record['Word i entity tag'])}
				_id += 1 

