import pandas as pd

WIKIDATA_P_BASIC_NORMALIZED = './data/wikidata_per_basic_norm.csv'
WIKIDATA_ORG_BASIC_NORMALIZED = './data/wikidata_org_basic_norm.csv'
WIKIDATA_LOC_BASIC_NORMALIZED = './data/wikidata_loc_basic_norm.csv'
GEONAMES_BASIC_NORMALIZED = './data/geonames_basic_norm.csv'
TOPONYM_P_BASIC_NORMALIZED = './data/toponym_per_basic_norm.csv'
TOPONYM_LOC_BASIC_NORMALIZED = './data/toponym_loc_basic_norm.csv'
TOPONYM_ORG_BASIC_NORMALIZED = './data/toponym_org_basic_norm.csv'

PER_OUT = './data/experiments/per_'
LOC_OUT = './data/experiments/loc_'
ORG_OUT = './data/experiments/org_'


n_test = 5000            # Test records (unique ids) taken from Wikidata 
random_state = 1001


df_p1 = pd.read_csv(WIKIDATA_P_BASIC_NORMALIZED, sep='|', encoding='utf-8', dtype=str)
df_p2 = pd.read_csv(TOPONYM_P_BASIC_NORMALIZED, sep='|', encoding='utf-8', dtype=str)

df_l1 = pd.read_csv(WIKIDATA_LOC_BASIC_NORMALIZED, sep='|', encoding='utf-8', dtype=str)
df_l2 = pd.read_csv(TOPONYM_LOC_BASIC_NORMALIZED, sep='|', encoding='utf-8', dtype=str)
df_l3 = pd.read_csv(GEONAMES_BASIC_NORMALIZED, sep='|', encoding='utf-8', dtype=str)

df_o1 = pd.read_csv(WIKIDATA_ORG_BASIC_NORMALIZED, sep='|', encoding='utf-8', dtype=str)
df_o2 = pd.read_csv(TOPONYM_ORG_BASIC_NORMALIZED, sep='|', encoding='utf-8', dtype=str)


df_per_test_ids = set(df_p1['id'].drop_duplicates().sample(n=n_test, random_state=random_state).to_list())
df_loc_test_ids = set(df_l1['id'].drop_duplicates().sample(n=n_test, random_state=random_state).to_list())
df_org_test_ids = set(df_o1['id'].drop_duplicates().sample(n=n_test, random_state=random_state).to_list())

df_per_test = df_p1[df_p1['id'].isin(df_per_test_ids)]
df_p1 = df_p1.drop(df_per_test.index)
df_loc_test = df_l1[df_l1['id'].isin(df_loc_test_ids)]
df_l1 = df_l1.drop(df_loc_test.index)
df_org_test = df_o1[df_o1['id'].isin(df_org_test_ids)]
df_o1 = df_o1.drop(df_org_test.index)

df_per_test[['id', 'input', 'target']].groupby(by=['id', 'input'], as_index=False).agg({'target':lambda x: list(x)}).to_csv(PER_OUT + 'test.csv', sep='|', encoding='utf-8', index=False)
df_loc_test[['id', 'input', 'target']].groupby(by=['id', 'input'], as_index=False).agg({'target':lambda x: list(x)}).to_csv(LOC_OUT + 'test.csv', sep='|', encoding='utf-8', index=False)
df_org_test[['id', 'input', 'target']].groupby(by=['id', 'input'], as_index=False).agg({'target':lambda x: list(x)}).to_csv(ORG_OUT + 'test.csv', sep='|', encoding='utf-8', index=False)

pd.concat([df_p1[['input', 'target']], df_p2[['input', 'target']]]).to_csv(PER_OUT + 'train_val.csv', sep='|', encoding='utf-8', index=False)
pd.concat([df_o1[['input', 'target']], df_o2[['input', 'target']]]).to_csv(ORG_OUT + 'train_val.csv', sep='|', encoding='utf-8', index=False)
pd.concat([df_l1[['input', 'target']], df_l2[['input', 'target']], df_l3[['input', 'target']]]).to_csv(LOC_OUT + 'train_val.csv', sep='|', encoding='utf-8', index=False)