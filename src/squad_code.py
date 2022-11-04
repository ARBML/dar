def get_squad_code(dataset_name, split_code):
    code = f'''
import json
from glob import glob 
import zipfile
import datasets

class {dataset_name}Config(datasets.BuilderConfig):

    def __init__(self, **kwargs):
        super({dataset_name}Config, self).__init__(**kwargs)


class {dataset_name}(datasets.GeneratorBasedBuilder):

    BUILDER_CONFIGS = [
        {dataset_name}Config(
            name="plain_text",
            version=datasets.Version("1.0.0", ""),
            description="Plain text",
        )
    ]

    def _info(self):
        return datasets.DatasetInfo(
            features=datasets.Features(
                {{
                    'id': datasets.Value('string'),
                    'title': datasets.Value('string'),
                    'context': datasets.Value('string'),
                    'question': datasets.Value('string'),
                    'answers': datasets.features.Sequence(
                        {{'text': datasets.Value('string'), 'answer_start': datasets.Value('int32')}}
                    ),
                }}
            ),
        )

    {split_code}

    def _generate_examples(self, filepaths):
        for i, filepath in enumerate(filepaths['inputs']): 
            with open(filepaths['inputs'][i], encoding='utf-8') as f:
                dataset = json.load(f)
                for article in dataset['data']:
                    title = article.get('title', '').strip()
                    for paragraph in article['paragraphs']:
                        context = paragraph['context'].strip()
                        for qa in paragraph['qas']:
                            question = qa['question'].strip()
                            id_ = qa['id']

                            answer_starts = [answer['answer_start'] for answer in qa['answers']]
                            answers = [answer['text'].strip() for answer in qa['answers']]

                            yield id_, {{
                                'title': title,
                                'context': context,
                                'question': question,
                                'id': id_,
                                'answers': {{'answer_start': answer_starts, 'text': answers}},
                            }}
    '''
    return code
