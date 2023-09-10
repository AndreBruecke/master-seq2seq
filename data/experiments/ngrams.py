import pandas as pd
import re
from nltk.tokenize import SyllableTokenizer


pairs_file = 'org_train_val_e2'
df = pd.read_csv(f'{pairs_file}.csv', sep='|', encoding='utf-8').dropna().sample(frac=0.85) # 0.5

t_split = pd.DataFrame({'term': df['target'].apply(lambda s: re.split(r' +| ?- ?', s)).explode(ignore_index=True)})
t_split['count'] = 1
print(t_split.groupby(by='term', as_index=False)['count'].count().sort_values(by='count', ascending=False).head(40))

"""
gen_grams = []
gen_syllables = []

syl_tok = SyllableTokenizer()
for ix, tpl in enumerate(df.itertuples()):
    input_3_grams = set([tpl.input[i: i + 3] for i in range(len(tpl.input) - 3 + 1)])
    gen_3_grams = set([tpl.target[i: i + 3] for i in range(len(tpl.target) - 3 + 1)])
    gen_grams.extend([g for g in gen_3_grams.difference(input_3_grams) if ' ' not in g])
    input_syl = set(sum([syl_tok.tokenize(token) for token in tpl.input.split(' ')], []))
    gen_syl = set(sum([syl_tok.tokenize(token) for token in tpl.target.split(' ')], []))
    gen_syllables.extend(gen_syl.difference(input_syl))

generated_3_grams = pd.DataFrame({'3grams': gen_grams})
generated_3_grams['count'] = 1
generated_3_grams = generated_3_grams.groupby(by='3grams').count().sort_values(by='count', ascending=False)
generated_3_grams.to_csv(f'{pairs_file}__3grams.csv', sep='|', encoding='utf-8', index=True)

generated_syl = pd.DataFrame({'syllables': gen_syllables})
generated_syl['count'] = 1
generated_syl = generated_syl.groupby(by='syllables').count().sort_values(by='count', ascending=False)
generated_syl.to_csv(f'{pairs_file}__syl.csv', sep='|', encoding='utf-8', index=True)
"""