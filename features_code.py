def get_features_code(columns, labels, set_label = False):
  
  features = ','.join([f"'{feature}':datasets.Value('string')" for feature in columns])
  if set_label:
    label = f",'label': datasets.features.ClassLabel(names={str(labels)})"
  else:
    label = ""
  func_name = 'def _info(self):\n'
  func_body = "return datasets.DatasetInfo(features=datasets.Features({"+ features + label+"}))"
  return f"\t{func_name}\t\t{func_body}\n"