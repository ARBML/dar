import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ArVox(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'audio':datasets.Audio(sampling_rate=16_000),'label': datasets.features.ClassLabel(names=['Persian', 'kabyl', 'English', 'Arabic', 'German'])}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/ArVox Dataset/**.wav')),} })]


	def get_label_from_path(self, labels, label):
		for l in labels:
			if l == label:
				return label

	def read_wav(self, filepath):
		if filepath.endswith('.wav') or filepath.endswith('.mp3'):
			raw_data = {'audio':[filepath]}
		else:
			raw_data = {'text':[open(filepath).read()]}
		return pd.DataFrame(raw_data)

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_wav(filepath)
			if len(df.columns) != 1:
				continue
			df.columns = ['audio']
			label = self.get_label_from_path(['Persian', 'kabyl', 'English', 'Arabic', 'German'], filepath.split('/')[-1].split('.')[-2])
			for _, record in df.iterrows():
				yield str(_id), {'audio':record['audio'],'label':str(label)}
				_id += 1 

