import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class AOC_ALDi(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'annotator_city_from_IP':datasets.Value('string'),'annotator_country_from_IP':datasets.Value('string'),'annotator_dialect':datasets.Value('string'),'annotator_id':datasets.Value('string'),'annotator_residence':datasets.Value('string'),'annotator_native_arabic_speaker':datasets.Value('string'),'average_dialectness_level':datasets.Value('string'),'comment_id':datasets.Value('string'),'dialect_level':datasets.Value('string'),'dialect':datasets.Value('string'),'dialectness_level':datasets.Value('string'),'document':datasets.Value('string'),'ratio_junk_or_missing':datasets.Value('string'),'length':datasets.Value('string'),'number_annotations':datasets.Value('string'),'same_label':datasets.Value('string'),'same_polarity':datasets.Value('string'),'sentence':datasets.Value('string'),'source':datasets.Value('string'),'comments_per_document':datasets.Value('string')}))

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
		url = ['https://raw.githubusercontent.com/AMR-KELEG/ALDi/master/data/AOC-ALDi.tar.gz']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'AOC-ALDi/train.tsv'),]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'AOC-ALDi/test.tsv'),]} }),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'AOC-ALDi/dev.tsv'),]} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			if len(df.columns) != 20:
				continue
			df.columns = ['annotator_city_from_IP', 'annotator_country_from_IP', 'annotator_dialect', 'annotator_id', 'annotator_residence', 'annotator_native_arabic_speaker', 'average_dialectness_level', 'comment_id', 'dialect_level', 'dialect', 'dialectness_level', 'document', 'ratio_junk_or_missing', 'length', 'number_annotations', 'same_label', 'same_polarity', 'sentence', 'source', 'comments_per_document']
			for _, record in df.iterrows():
				yield str(_id), {'annotator_city_from_IP':record['annotator_city_from_IP'],'annotator_country_from_IP':record['annotator_country_from_IP'],'annotator_dialect':record['annotator_dialect'],'annotator_id':record['annotator_id'],'annotator_residence':record['annotator_residence'],'annotator_native_arabic_speaker':record['annotator_native_arabic_speaker'],'average_dialectness_level':record['average_dialectness_level'],'comment_id':record['comment_id'],'dialect_level':record['dialect_level'],'dialect':record['dialect'],'dialectness_level':record['dialectness_level'],'document':record['document'],'ratio_junk_or_missing':record['ratio_junk_or_missing'],'length':record['length'],'number_annotations':record['number_annotations'],'same_label':record['same_label'],'same_polarity':record['same_polarity'],'sentence':record['sentence'],'source':record['source'],'comments_per_document':record['comments_per_document']}
				_id += 1 

