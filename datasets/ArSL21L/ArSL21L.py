import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class ArSL21L(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'image':datasets.Image(),'label': datasets.features.ClassLabel(names=['ain', 'al', 'aleff','bb','dal','dha','dhad','fa','gaaf','ghain','ha','haa','jeem','kaaf','khaa','la','laam',
        'meem','nun','ra','saad','seen','sheen','ta','taa','thaa','thal','toot','waw','ya','yaa','zay'])}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':sorted(glob(downloaded_files[0]+'/sign_data/images/**.jpg')),'targets1':sorted(glob(downloaded_files[0]+'/sign_data/labels/**.txt')),} })]


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
			dfs = [df] 
			dfs.append(self.read_image(filepaths['targets1'][i]))
			df = pd.concat(dfs, axis = 1)
			if len(df.columns) != 2:
				continue
			df = df[['image', 'text']]
			for _, record in df.iterrows():
				yield str(_id), {'image':record['image'],'label':int(record['text'].split(' ')[0])}
				_id += 1 

