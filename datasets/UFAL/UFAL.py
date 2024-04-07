import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class UFAL(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'arb':datasets.Value('string'),'eng':datasets.Value('string'),'apc':datasets.Value('string'),'fra':datasets.Value('string'),'spa':datasets.Value('string'),'ell':datasets.Value('string'),'deu':datasets.Value('string')}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/ufal-nla-v1.arb')),'targets1':sorted(glob(downloaded_files[0]+'/ufal-nla-v1.eng')),'targets2':sorted(glob(downloaded_files[0]+'/ufal-nla-v1.apc')),'targets3':sorted(glob(downloaded_files[0]+'/ufal-nla-v1.fra')),'targets4':sorted(glob(downloaded_files[0]+'/ufal-nla-v1.spa')),'targets5':sorted(glob(downloaded_files[0]+'/ufal-nla-v1.ell')),'targets6':sorted(glob(downloaded_files[0]+'/ufal-nla-v1.deu')),} })]


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
			dfs.append(self.read_txt(filepaths['targets4'][i], skiprows = 0, lines = True, encoding = 'utf-8'))
			dfs.append(self.read_txt(filepaths['targets5'][i], skiprows = 0, lines = True, encoding = 'utf-8'))
			dfs.append(self.read_txt(filepaths['targets6'][i], skiprows = 0, lines = True, encoding = 'utf-8'))
			df = pd.concat(dfs, axis = 1)
			if len(df.columns) != 7:
				continue
			df.columns = ['arb', 'eng', 'apc', 'fra', 'spa', 'ell', 'deu']
			df = df[['arb', 'eng', 'apc', 'fra', 'spa', 'ell', 'deu']]
			for _, record in df.iterrows():
				yield str(_id), {'arb':record['arb'],'eng':record['eng'],'apc':record['apc'],'fra':record['fra'],'spa':record['spa'],'ell':record['ell'],'deu':record['deu']}
				_id += 1 

