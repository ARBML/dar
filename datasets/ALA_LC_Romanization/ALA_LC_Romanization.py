import os
import pandas as pd 
import datasets

class ALA_LC_Romanization(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'sentID':datasets.Value('string'),'rom':datasets.Value('string'),'ar':datasets.Value('string'),'rom_raw':datasets.Value('string'),'ar_raw':datasets.Value('string'),'comb.tag':datasets.Value('string'),'recID':datasets.Value('string'),'subtag':datasets.Value('string'),'tag':datasets.Value('string'),'link':datasets.Value('string'),'source':datasets.Value('string'),'splits':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['https://github.com/CAMeL-Lab/Arabic_ALA-LC_Romanization/releases/download/v1.0/data.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],'data/processed/train.tsv')]}),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],'data/processed/test.tsv')]}),datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],'data/processed/dev.tsv')]})]
	def _generate_examples(self, filepaths):
		_id = 0
		for filepath in filepaths:
			df = pd.read_csv(open(filepath, 'rb'), sep = '\t', skiprows = 0, error_bad_lines = False, header = 0)
			df.columns = ['sentID', 'rom', 'ar', 'rom_raw', 'ar_raw', 'comb.tag', 'recID', 'subtag', 'tag', 'link', 'source', 'splits']
			for _, record in df.iterrows():
				yield str(_id), {'sentID':record['sentID'],'rom':record['rom'],'ar':record['ar'],'rom_raw':record['rom_raw'],'ar_raw':record['ar_raw'],'comb.tag':record['comb.tag'],'recID':record['recID'],'subtag':record['subtag'],'tag':record['tag'],'link':record['link'],'source':record['source'],'splits':record['splits']}
				_id += 1 

