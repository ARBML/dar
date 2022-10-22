import json

import datasets
from datasets.tasks import QuestionAnsweringExtractive


logger = datasets.logging.get_logger(__name__)

_URLs = {'train': 'https://raw.githubusercontent.com/adelmeleka/AQAD/master/AQQAD%201.0/FINAL_AAQAD-v1.0.json'}
class AQADConfig(datasets.BuilderConfig):
    """BuilderConfig for AQAD."""

    def __init__(self, **kwargs):
        """BuilderConfig for AQAD.
        Args:
          **kwargs: keyword arguments forwarded to super.
        """
        super(AQADConfig, self).__init__(**kwargs)


class AQAD(datasets.GeneratorBasedBuilder):

    BUILDER_CONFIGS = [
        AQADConfig(
            name="plain_text",
            version=datasets.Version("1.0.0", ""),
            description="Plain text",
        )
    ]

    def _info(self):
        return datasets.DatasetInfo(
            features=datasets.Features(
                {
                    "id": datasets.Value("string"),
                    "title": datasets.Value("string"),
                    "context": datasets.Value("string"),
                    "question": datasets.Value("string"),
                    "answers": datasets.features.Sequence(
                        {"text": datasets.Value("string"), "answer_start": datasets.Value("int32")}
                    ),
                }
            ),
            # No default supervised_keys (as we have to pass both question
            # and context as input).
            supervised_keys=None,
            task_templates=[
                QuestionAnsweringExtractive(
                    question_column="question", context_column="context", answers_column="answers"
                )
            ],
        )

    def _split_generators(self, dl_manager):
        urls_to_download = _URLs
        downloaded_files = dl_manager.download_and_extract(urls_to_download)

        return [
            datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={"filepath": downloaded_files["train"]}),
        ]

    def _generate_examples(self, filepath):
        """This function returns the examples in the raw (text) form."""
        logger.info("generating examples from = %s", filepath)
        with open(filepath, encoding="utf-8") as f:
            arcd = json.load(f)
            for article in arcd["data"]:
                title = article.get("title", "").strip()
                for paragraph in article["paragraphs"]:
                    context = paragraph["context"].strip()
                    for qa in paragraph["qas"]:
                        question = qa["question"].strip()
                        id_ = qa["id"]

                        answer_starts = [answer["answer_start"] for answer in qa["answers"]]
                        answers = [answer["text"].strip() for answer in qa["answers"]]

                        # Features currently used are "context", "question", and "answers".
                        # Others are extracted here for the ease of future expansions.
                        yield id_, {
                            "title": title,
                            "context": context,
                            "question": question,
                            "id": id_,
                            "answers": {"answer_start": answer_starts, "text": answers},
                        }