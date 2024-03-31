import os
import pandas as pd 
import datasets
from glob import glob
import zipfile

class Corpus_of_Offensive_Language_in_Arabic(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Index':datasets.Value('string'),'IndexVidComments':datasets.Value('string'),'id':datasets.Value('string'),'user':datasets.Value('string'),'date':datasets.Value('string'),'timestamp':datasets.Value('string'),'commentText':datasets.Value('string'),'Unnamed: 7':datasets.Value('string'),'likes':datasets.Value('string'),'hasReplies':datasets.Value('string'),'numberOfReplies':datasets.Value('string'),'replies.id':datasets.Value('string'),'replies.user':datasets.Value('string'),'replies.date':datasets.Value('string'),'replies.timestamp':datasets.Value('string'),'replies.commentText':datasets.Value('string'),'replies.likes':datasets.Value('string'),'annotator1':datasets.Value('string'),'annotator2':datasets.Value('string'),'annotator3':datasets.Value('string'),'label': datasets.features.ClassLabel(names=['P', 'N'])}))

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
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': {'inputs':downloaded_files} })]


	def _generate_examples(self, filepaths):
		_id = 0
		for i,filepath in enumerate(filepaths['inputs']):
			df = pd.read_excel(open(filepath, 'rb'), skiprows = 0, header = 0)
			if len(df.columns) != 21:
				continue
			df.columns = ['Index', 'IndexVidComments', 'id', 'user', 'date', 'timestamp', 'commentText', 'Unnamed: 7', 'likes', 'hasReplies', 'numberOfReplies', 'replies.id', 'replies.user', 'replies.date', 'replies.timestamp', 'replies.commentText', 'replies.likes', 'annotator1', 'annotator2', 'annotator3', 'Label']
			for _, record in df.iterrows():
				yield str(_id), {'Index':record['Index'],'IndexVidComments':record['IndexVidComments'],'id':record['id'],'user':record['user'],'date':record['date'],'timestamp':record['timestamp'],'commentText':record['commentText'],'Unnamed: 7':record['Unnamed: 7'],'likes':record['likes'],'hasReplies':record['hasReplies'],'numberOfReplies':record['numberOfReplies'],'replies.id':record['replies.id'],'replies.user':record['replies.user'],'replies.date':record['replies.date'],'replies.timestamp':record['replies.timestamp'],'replies.commentText':record['replies.commentText'],'replies.likes':record['replies.likes'],'annotator1':record['annotator1'],'annotator2':record['annotator2'],'annotator3':record['annotator3'],'label':str(record['Label'])}
				_id += 1 

