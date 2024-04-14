import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class CALLHOME(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'r1':datasets.Value('string'),'r2':datasets.Value('string'),'r3':datasets.Value('string'),'r4':datasets.Value('string')}))

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
		url = ['https://github.com/noisychannel/ARZ_callhome_corpus/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_dev.en.0'),],'targets1':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_dev.en.1'),],'targets2':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_dev.en.2'),],'targets3':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_dev.en.3'),]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_test.en.0'),],'targets1':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_test.en.1'),],'targets2':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_test.en.2'),],'targets3':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_test.en.3'),]} }),datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_train.en.0'),],'targets1':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_train.en.1'),],'targets2':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_train.en.2'),],'targets3':[os.path.join(downloaded_files[0],'ARZ_callhome_corpus-master/corpus/callhome_train.en.3'),]} })]


	def read_txt(self, filepath, skiprows = 0, lines = True, encoding = 'utf-8'):
		if lines:
			return pd.DataFrame(open(filepath, 'r', encoding = encoding).read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r', encoding = encoding).read()])

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = True, encoding = 'utf-8')
			dfs = [df] 
			dfs.append(self.read_txt(filepaths['targets1'][i], skiprows = 0, lines = True, encoding = 'utf-8'))
			dfs.append(self.read_txt(filepaths['targets2'][i], skiprows = 0, lines = True, encoding = 'utf-8'))
			dfs.append(self.read_txt(filepaths['targets3'][i], skiprows = 0, lines = True, encoding = 'utf-8'))
			df = pd.concat(dfs, axis = 1)
			if len(df.columns) != 4:
				continue
			df.columns = ['r1', 'r2', 'r3', 'r4']
			df = df[['r1', 'r2', 'r3', 'r4']]
			for _, record in df.iterrows():
				yield str(_id), {'r1':record['r1'],'r2':record['r2'],'r3':record['r3'],'r4':record['r4']}
				_id += 1 

