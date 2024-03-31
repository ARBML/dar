import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class QADI(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'tweet_id':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['QADI_train_ids_QA', 'QADI_train_ids_YE', 'QADI_train_ids_BH', 'QADI_train_ids_IQ', 'QADI_train_ids_KW', 'QADI_train_ids_AE', 'QADI_train_ids_MA', 'QADI_train_ids_DZ', 'QADI_train_ids_SY', 'QADI_train_ids_LY', 'QADI_train_ids_JO', 'QADI_train_ids_LB', 'QADI_train_ids_OM', 'QADI_train_ids_EG', 'QADI_train_ids_TN', 'QADI_train_ids_SA', 'QADI_train_ids_SD', 'QADI_train_ids_PL'])}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'dataset/QADI_train_ids_AE.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_BH.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_DZ.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_EG.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_IQ.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_JO.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_KW.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_LB.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_LY.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_MA.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_OM.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_PL.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_QA.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_SA.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_SD.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_SY.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_TN.txt'),os.path.join(downloaded_files[0],'dataset/QADI_train_ids_YE.txt'),]} })]


	def get_label_from_path(self, labels, label):
		for l in labels:
			if l == label:
				return label

	def read_txt(self, filepath, skiprows = 0, lines = True, encoding = 'utf-8'):
		if lines:
			return pd.DataFrame(open(filepath, 'r', encoding = encoding).read().splitlines()[skiprows:])
		else:
			return pd.DataFrame([open(filepath, 'r', encoding = encoding).read()])

	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = self.read_txt(filepath, skiprows = 0, lines = True, encoding = 'utf-8')
			if len(df.columns) != 1:
				continue
			df.columns = ['tweet_id']
			label = self.get_label_from_path(['QADI_train_ids_QA', 'QADI_train_ids_YE', 'QADI_train_ids_BH', 'QADI_train_ids_IQ', 'QADI_train_ids_KW', 'QADI_train_ids_AE', 'QADI_train_ids_MA', 'QADI_train_ids_DZ', 'QADI_train_ids_SY', 'QADI_train_ids_LY', 'QADI_train_ids_JO', 'QADI_train_ids_LB', 'QADI_train_ids_OM', 'QADI_train_ids_EG', 'QADI_train_ids_TN', 'QADI_train_ids_SA', 'QADI_train_ids_SD', 'QADI_train_ids_PL'], filepath.split('/')[-1].split('.')[-2])
			for _, record in df.iterrows():
				yield str(_id), {'tweet_id':record['tweet_id'],'label':str(label)}
				_id += 1 

