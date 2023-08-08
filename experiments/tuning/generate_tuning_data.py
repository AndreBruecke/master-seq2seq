import pandas as pd
import os

from unidecode import unidecode

maxlen = 60

# LOC
loc_toponym = pd.read_csv('similarity_toponym_loc_sample.csv', sep='\t', encoding='utf-8', header=None, names=['input', 'target', 'label'], dtype=str)
loc_toponym['input'] = loc_toponym['input'].apply(unidecode)
loc_toponym['target'] = loc_toponym['target'].apply(unidecode)
loc_toponym = loc_toponym[(loc_toponym['label'] == 'TRUE') & (loc_toponym['input'] != loc_toponym['target'])]
loc_toponym = loc_toponym[(loc_toponym['input'].str.len() < maxlen) & (loc_toponym['target'].str.len() < maxlen)]

loc_geonames = pd.read_csv('uncertain_pairs_geonames_balanced.csv', sep='\t', encoding='utf-8', header=None, names=['input', 'target', 'label'], dtype=str)
loc_geonames['input'] = loc_geonames['input'].apply(unidecode)
loc_geonames['target'] = loc_geonames['target'].apply(unidecode)
loc_geonames = loc_geonames[(loc_geonames['label'] == 'TRUE')  & (loc_geonames['input'] != loc_geonames['target'])]
loc_geonames = loc_geonames[(loc_geonames['input'].str.len() < maxlen) & (loc_geonames['target'].str.len() < maxlen)]

loc_wiki = pd.read_csv('uncertain_pairs_loc_balanced.csv', sep='\t', encoding='utf-8', header=None, names=['input', 'target', 'label'], dtype=str)
loc_wiki['input'] = loc_wiki['input'].apply(unidecode)
loc_wiki['target'] = loc_wiki['target'].apply(unidecode)
loc_wiki = loc_wiki[(loc_wiki['label'] == 'TRUE') & (loc_wiki['input'] != loc_wiki['target'])]
loc_wiki = loc_wiki[(loc_wiki['input'].str.len() < maxlen) & (loc_wiki['target'].str.len() < maxlen)]

# PER
per_toponym = pd.read_csv('similarity_toponym_p_sample.csv', sep='\t', encoding='utf-8', header=None, names=['input', 'target', 'label'], dtype=str)
per_toponym['input'] = per_toponym['input'].apply(unidecode)
per_toponym['target'] = per_toponym['target'].apply(unidecode)
per_toponym = per_toponym[(per_toponym['label'] == 'TRUE') & (per_toponym['input'] != per_toponym['target'])]
per_toponym = per_toponym[(per_toponym['input'].str.len() < maxlen) & (per_toponym['target'].str.len() < maxlen)]

per_wiki = pd.read_csv('uncertain_pairs_p_balanced.csv', sep='\t', encoding='utf-8', header=None, names=['input', 'target', 'label'], dtype=str)
per_wiki['input'] = per_wiki['input'].apply(unidecode)
per_wiki['target'] = per_wiki['target'].apply(unidecode)
per_wiki = per_wiki[(per_wiki['label'] == 'TRUE') & (per_wiki['input'] != per_wiki['target'])]
per_wiki = per_wiki[(per_wiki['input'].str.len() < maxlen) & (per_wiki['target'].str.len() < maxlen)]

# ORG
org_toponym = pd.read_csv('similarity_toponym_org_sample.csv', sep='\t', encoding='utf-8', header=None, names=['input', 'target', 'label'], dtype=str)
org_toponym['input'] = org_toponym['input'].apply(unidecode)
org_toponym['target'] = org_toponym['target'].apply(unidecode)
org_toponym = org_toponym[(org_toponym['label'] == 'TRUE') & (org_toponym['input'] != org_toponym['target'])]
org_toponym = org_toponym[(org_toponym['input'].str.len() < maxlen) & (org_toponym['target'].str.len() < maxlen)]

org_wiki = pd.read_csv('uncertain_pairs_org_balanced.csv', sep='\t', encoding='utf-8', header=None, names=['input', 'target', 'label'], dtype=str)
org_wiki['input'] = org_wiki['input'].apply(unidecode)
org_wiki['target'] = org_wiki['target'].apply(unidecode)
org_wiki = org_wiki[(org_wiki['label'] == 'TRUE') & (org_wiki['input'] != org_wiki['target'])]
org_wiki = org_wiki[(org_wiki['input'].str.len() < maxlen) & (org_wiki['target'].str.len() < maxlen)]

print("PER: ", len(per_toponym) + len(per_wiki))
print("ORG: ", len(org_toponym) + len(org_wiki))
print("GEO:", len(loc_toponym) + len(loc_wiki) + len(loc_geonames))

random_state = 1000

per_train_val = pd.concat([per_toponym.sample(n=16666, random_state=random_state), per_wiki.sample(n=16667, random_state=random_state)])
per_train_val['label'] = 'PER'
loc_train_val = pd.concat([loc_toponym.sample(n=11111, random_state=random_state), loc_wiki.sample(n=11112, random_state=random_state), loc_geonames.sample(n=11111, random_state=random_state)])
loc_train_val['label'] = 'LOC'
org_train_val = pd.concat([org_toponym.sample(n=16666, random_state=random_state), org_wiki.sample(n=16667, random_state=random_state)])
org_train_val['label'] = 'ORG'

per_test = pd.concat([per_toponym, per_wiki]).drop(per_train_val.index).sample(n=6667, random_state=random_state)
per_test['label'] = 'PER'
loc_test = pd.concat([loc_toponym, loc_wiki, loc_geonames]).drop(loc_train_val.index).sample(n=6667, random_state=random_state)
loc_test['label'] = 'LOC'
org_test = pd.concat([org_toponym, org_wiki]).drop(org_train_val.index).sample(n=6666, random_state=random_state)
org_test['label'] = 'ORG'

train_val_df = pd.concat([per_train_val, loc_train_val, org_train_val]).sample(frac=1, random_state=random_state)
train_val_df.to_csv('tuning_train_val.csv', sep='|', encoding='utf-8', index=False)
print(len(train_val_df))
test_df = pd.concat([per_test, loc_test, org_test]).sample(frac=1, random_state=random_state)
test_df.to_csv('tuning_test.csv', sep='|', encoding='utf-8', index=False)
print(len(test_df))
