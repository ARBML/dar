def get_config_code(DATASET_NAME):
  return f"class {DATASET_NAME}Config(datasets.BuilderConfig):\n"\
          +"\tdef __init__(self, **kwargs):\n"\
          +f"\t\tsuper({DATASET_NAME}Config, self).__init__(**kwargs)"
