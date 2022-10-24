def get_features_code(columns, label_names):
  
  features = ','.join([f"'{feature}':datasets.Value('string')" for feature in columns])
  if label_names is not None:
    label = f",'label': datasets.features.ClassLabel(names={str(label_names)})"
  else:
    label = ""
  func_name = 'def _info(self):\n'
  func_body = "return datasets.DatasetInfo(features=datasets.Features({"+ features + label+"}))"
  return f"\t{func_name}\t\t{func_body}\n"