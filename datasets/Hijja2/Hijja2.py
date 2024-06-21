import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class Hijja2(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'image':datasets.Image(),'label': datasets.features.ClassLabel(names=['4 tha', '3 ta', '9 thal', '11 zay', '20 fa', '6 ha', '18 ayn', '16 da', '21 qaf', '24 mim', '23 lam', '17 za', '2 ba', '14 sad', '13 shin', '7 kha', '26 ha', '29 hamza', '28 ya', '22 kaf', '10 ra', '15 dad', '12 sin', '19 gayn', '1 alif', '25 non', '27 waw', '5 gim', '8 dal'])}))

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
		url = ['https://github.com/israksu/Hijja2/archive/master.zip']
		downloaded_files = dl_manager.download_and_extract(url)
		self.extract_all(downloaded_files[0])
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/Hijja2-master/**/**/**.png')),} })]


	def get_label_from_path(self, labels, label):
		for l in labels:
			if l == label:
				return label

	def read_image(self, filepath):
		if filepath.endswith('.jpg') or filepath.endswith('.png'):
			raw_data = {'image':[filepath]}
		else:
			raw_data = {'text':[open(filepath).read()]}
		return pd.DataFrame(raw_data)

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_image(filepath)
			if len(df.columns) != 1:
				continue
			df = df[['image']]
			label = self.get_label_from_path(['4 tha', '3 ta', '9 thal', '11 zay', '20 fa', '6 ha', '18 ayn', '16 da', '21 qaf', '24 mim', '23 lam', '17 za', '2 ba', '14 sad', '13 shin', '7 kha', '26 ha', '29 hamza', '28 ya', '22 kaf', '10 ra', '15 dad', '12 sin', '19 gayn', '1 alif', '25 non', '27 waw', '5 gim', '8 dal'], filepath.split('/')[-3])
			for _, record in df.iterrows():
				yield str(_id), {'image':record['image'],'label':str(label)}
				_id += 1 

