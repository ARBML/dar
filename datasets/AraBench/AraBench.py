import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class AraBench(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Arabic':datasets.Value('string'),'English':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['jo', 'iq', 'sa', 'tn', 'eg', 'ye', 'lb', 'sd', 'ms', 'pa', 'om', 'ma', 'sy', 'ly', 'dz', 'qa'])}))

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
		url = ['https://alt.qcri.org/resources/mt/arabench/releases/current/AraBench_dataset.tgz']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.VALIDATION, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'AraBench_dataset/QAraC.dev.glf.0.qa.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.mgr.0.ma.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.mgr.0.tn.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.msa.0.ms.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.msa.1.ms.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.glf.0.qa.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.lev.0.lb.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.mgr.0.ma.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.mgr.0.tn.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.msa.0.ms.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.nil.0.eg.ar'),],'targets1':[os.path.join(downloaded_files[0],'AraBench_dataset/QAraC.dev.glf.0.qa.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.mgr.0.ma.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.mgr.0.tn.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.msa.0.ms.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.dev.msa.1.ms.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.glf.0.qa.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.lev.0.lb.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.mgr.0.ma.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.mgr.0.tn.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.msa.0.ms.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.dev.nil.0.eg.en'),]} }),datasets.SplitGenerator(name=datasets.Split.TEST, gen_kwargs={'filepaths':{'inputs':[os.path.join(downloaded_files[0],'AraBench_dataset/QAraC.test.glf.0.qa.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.mgr.0.ma.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.mgr.0.tn.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.msa.0.ms.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.msa.1.ms.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.iq.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.om.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.qa.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.sa.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.ye.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.1.iq.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.1.sa.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.2.iq.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.jo.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.lb.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.pa.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.sy.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.1.jo.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.1.sy.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.dz.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.ly.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.ma.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.tn.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.1.ly.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.1.ma.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.1.tn.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.msa.0.ms.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.0.eg.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.0.sd.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.1.eg.ar'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.2.eg.ar'),],'targets1':[os.path.join(downloaded_files[0],'AraBench_dataset/QAraC.test.glf.0.qa.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.mgr.0.ma.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.mgr.0.tn.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.msa.0.ms.en'),os.path.join(downloaded_files[0],'AraBench_dataset/bible.test.msa.1.ms.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.iq.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.om.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.qa.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.sa.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.0.ye.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.1.iq.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.1.sa.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.glf.2.iq.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.jo.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.lb.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.pa.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.0.sy.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.1.jo.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.lev.1.sy.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.dz.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.ly.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.ma.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.0.tn.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.1.ly.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.1.ma.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.mgr.1.tn.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.msa.0.ms.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.0.eg.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.0.sd.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.1.eg.en'),os.path.join(downloaded_files[0],'AraBench_dataset/madar.test.nil.2.eg.en'),]} })]


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
			dfs = [df] 
			dfs.append(self.read_txt(filepaths['targets1'][i], skiprows = 0, lines = True, encoding = 'utf-8'))
			df = pd.concat(dfs, axis = 1)
			if len(df.columns) != 2:
				continue
			df.columns = ['Arabic', 'English']
			df = df[['Arabic', 'English']]
			label = self.get_label_from_path(['jo', 'iq', 'sa', 'tn', 'eg', 'ye', 'lb', 'sd', 'ms', 'pa', 'om', 'ma', 'sy', 'ly', 'dz', 'qa'], filepath.split('/')[-1].split('.')[-2])
			for _, record in df.iterrows():
				yield str(_id), {'Arabic':record['Arabic'],'English':record['English'],'label':str(label)}
				_id += 1 

