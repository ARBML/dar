import os
import pandas as pd 
import datasets

class Ajdir(datasets.GeneratorBasedBuilder):
	def _info(self):
		return datasets.DatasetInfo(features=datasets.Features({'Text':datasets.Value('string')}))
	def _split_generators(self, dl_manager):
		url = ['http://aracorpus.e3rab.com/argistestsrv.nmsu.edu/AraCorpus.tar.gz']
		downloaded_files = dl_manager.download_and_extract(url)
		return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={'filepaths': [os.path.join(downloaded_files[0],f) for f in ['AraCorpus/Data/Collection-www.alwatan.com.list.txt', 'AraCorpus/Data/Collection-www.addustour.com.list.txt', 'AraCorpus/Data/Collection-www.raya.com.list.txt', 'AraCorpus/Data/Collection-www.petra.gov.jo.list.txt', 'AraCorpus/Data/Collection-www.ly2day.com.list.txt', 'AraCorpus/Data/Collection-www.aps.dz.list.txt', 'AraCorpus/Data/Collection-news.bbc.co.uk.list.txt', 'AraCorpus/Data/Collection-www.rayaam.net.list.txt', 'AraCorpus/Data/Collection-www.omannews.com.list.txt', 'AraCorpus/Data/Collection-www.annaharonline.com.list.txt', 'AraCorpus/Data/Collection-thawra.com.list.txt', 'AraCorpus/Data/Collection-www.thisissyria.net.list.txt', 'AraCorpus/Data/Collection-saharamedia.net.list.txt', 'AraCorpus/Data/Collection-www.ahdath.info.list.txt', 'AraCorpus/Data/Collection-www.alquds.com.list.txt', 'AraCorpus/Data/Collection-www.alwatan.com.kw.list.txt', 'AraCorpus/Data/Collection-arabic.cnn.com.list.txt', 'AraCorpus/Data/Collection-www.al-jazirah.com.list.txt', 'AraCorpus/Data/Collection-www.alalam.ma.list.txt', 'AraCorpus/Data/Collection-www.almshaheer.com.list.txt', 'AraCorpus/Data/Collection-www.asharqalawsat.com.list.txt', 'AraCorpus/Data/Collection-www.assabah.com.tn.list.txt', 'AraCorpus/Data/Collection-www.al-jazirah.com.sa.list.txt', 'AraCorpus/Data/Collection-www.akhbar-libya.com.list.txt', 'AraCorpus/Data/Collection-www.attajdid.ma.list.txt', 'AraCorpus/Data/Collection-www.al-watan.com.list.txt', 'AraCorpus/Data/Collection-www.ahram.org.eg.list.txt']]})]
	def _generate_examples(self, filepaths):
		_id = 0
		labels = None
		for i,filepath in enumerate(filepaths):
			df = pd.read_csv(open(filepath, 'rb'), sep = ',', skiprows = 0, error_bad_lines = False, header = None, engine = 'python')
			df.columns = ['Text']
			for _, record in df.iterrows():
				yield str(_id), {'Text':record['Text']}
				_id += 1 

