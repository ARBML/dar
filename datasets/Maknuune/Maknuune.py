import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class Maknuune(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'ID':datasets.Value('string'),'ROOT':datasets.Value('string'),'ROOT_NTWS':datasets.Value('string'),'ROOT_1':datasets.Value('string'),'LEMMA':datasets.Value('string'),'LEMMA_SEARCH':datasets.Value('string'),'FORM':datasets.Value('string'),'LEMMA_BW':datasets.Value('string'),'FORM_BW':datasets.Value('string'),'CAPHI++':datasets.Value('string'),'ANALYSIS':datasets.Value('string'),'GLOSS':datasets.Value('string'),'GLOSS_MSA':datasets.Value('string'),'EXAMPLE_USAGE':datasets.Value('string'),'NOTES':datasets.Value('string'),'SOURCE':datasets.Value('string'),'ANNOTATOR':datasets.Value('string')}))

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
		url = ['https://drive.google.com/uc?id=1prIUi6nw9DHVkvBx0YiVcm6aQYYfqXLy&export=download']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/maknuune-v1.0.1/maknuune-v1.0.1.tsv')),} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_csv(filepath, sep = r'\t', skiprows = 0, error_bad_lines = False, header = 0, engine = 'python')
			if len(df.columns) != 17:
				continue
			df = df[['ID', 'ROOT', 'ROOT_NTWS', 'ROOT_1', 'LEMMA', 'LEMMA_SEARCH', 'FORM', 'LEMMA_BW', 'FORM_BW', 'CAPHI++', 'ANALYSIS', 'GLOSS', 'GLOSS_MSA', 'EXAMPLE_USAGE', 'NOTES', 'SOURCE', 'ANNOTATOR']]
			for _, record in df.iterrows():
				yield str(_id), {'ID':record['ID'],'ROOT':record['ROOT'],'ROOT_NTWS':record['ROOT_NTWS'],'ROOT_1':record['ROOT_1'],'LEMMA':record['LEMMA'],'LEMMA_SEARCH':record['LEMMA_SEARCH'],'FORM':record['FORM'],'LEMMA_BW':record['LEMMA_BW'],'FORM_BW':record['FORM_BW'],'CAPHI++':record['CAPHI++'],'ANALYSIS':record['ANALYSIS'],'GLOSS':record['GLOSS'],'GLOSS_MSA':record['GLOSS_MSA'],'EXAMPLE_USAGE':record['EXAMPLE_USAGE'],'NOTES':record['NOTES'],'SOURCE':record['SOURCE'],'ANNOTATOR':record['ANNOTATOR']}
				_id += 1 

