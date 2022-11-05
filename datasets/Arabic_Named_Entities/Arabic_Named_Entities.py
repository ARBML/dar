import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class Arabic_Named_Entities(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'kiform':datasets.Value('string'),'formArabicPointed':datasets.Value('string'),'diac_confidence':datasets.Value('string'),'stable':datasets.Value('string'),'formDIN31635':datasets.Value('string'),'formUNGEGN':datasets.Value('string'),'sense_id':datasets.Value('string'),'occurrences@confidence':datasets.Value('string'),'InstanceOf':datasets.Value('string'),'ont':datasets.Value('string'),'english':datasets.Value('string')}))

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
		url = ['https://sourceforge.net/projects/arabicnes/files/ArabicNEs-1.1.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/ArabicNEs-1.1/arabic_ne-full-lexicon.txt')),} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = None)
			if len(df.columns) != 11:
				continue
			df.columns = ['kiform', 'formArabicPointed', 'diac_confidence', 'stable', 'formDIN31635', 'formUNGEGN', 'sense_id', 'occurrences@confidence', 'InstanceOf', 'ont', 'english']
			for _, record in df.iterrows():
				yield str(_id), {'kiform':record['kiform'],'formArabicPointed':record['formArabicPointed'],'diac_confidence':record['diac_confidence'],'stable':record['stable'],'formDIN31635':record['formDIN31635'],'formUNGEGN':record['formUNGEGN'],'sense_id':record['sense_id'],'occurrences@confidence':record['occurrences@confidence'],'InstanceOf':record['InstanceOf'],'ont':record['ont'],'english':record['english']}
				_id += 1 

