import os
import unicodedata as ud

from qwikidata.entity import WikidataItem
from qwikidata.json_dump import WikidataJsonDump

PROP_INSTANCE_OF = 'P31'
PROP_GENDER = 'P21'
PROP_BIRTH = 'P569'
PROP_GIVEN_NAME = 'P735'
PROP_FAMILY_NAME = 'P734'

QID_HUMAN = 'Q5'

latin_letters = {}
def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def parse_variants(en_label, entity_dict):
    label_rows = ''
    # alias_rows = ''

    label_rows += f"{entity_dict['id']}\ten\t{en_label}\n"
    for k, v in entity_dict['labels'].items():
        # If label is identical to English label: skip
        if v['value'].strip() == en_label or not only_roman_chars(v['value']):
            continue
        label_rows += f"{entity_dict['id']}\t{k}\t{v['value'].strip()}\n"

    # for k, v in entity_dict['aliases'].items():
    #     alias_rows += f"{entity_dict['id']}\t{k}\t{';'.join([alias['value'].strip() for alias in v])}\n"

    return label_rows #, alias_rows

def parse_attributes(entity: WikidataItem):
    claim_group = entity.get_truthy_claim_group(PROP_GENDER)
    gender_qids = [claim.mainsnak.datavalue.value['id'] for claim in claim_group if claim.mainsnak.snaktype == 'value']
    gender = '' if len(gender_qids) < 1 else gender_qids[0]
    gender = gender.replace('Q6581097', 'male').replace('Q6581072', 'female')

    claim_group = entity.get_truthy_claim_group(PROP_BIRTH)
    date_values = [claim.mainsnak.datavalue.value['time'] for claim in claim_group if claim.mainsnak.snaktype == 'value']
    date = '' if len(date_values) < 1 else date_values[0]
    date = date.split('T')[0]
    return gender, date


dump_path = 'T:/MasterData/wikidata_dump/wikidata-20220103-all.json.gz'
out_folder = 'T:/MasterData/wikidata_dump/'
reader = WikidataJsonDump(dump_path)

label_out_f = open(os.path.join(out_folder, 'human_labels.tsv'), 'w', encoding='utf-8')
label_out_f.write('id\tlang\tlabel\n')
stats_out_f = open(os.path.join(out_folder, 'human_stats.tsv'), 'w', encoding='utf-8')
stats_out_f.write('id\tgender\tdate_of_birth\titem_languages\n')
# alias_out_f = open(os.path.join(out_folder, 'human_aliases.tsv'), 'w', encoding='utf-8')
# alias_out_f.write('id\tlang\taliases\n')

n_entities = 0
n_failures = 0
for i, entity_dict in enumerate(reader):
    try:
        if entity_dict['type'] != 'item': 
            continue

        entity = WikidataItem(entity_dict)
        claim_group = entity.get_truthy_claim_group(PROP_INSTANCE_OF)
        instance_qids = [claim.mainsnak.datavalue.value['id'] for claim in claim_group if claim.mainsnak.snaktype == 'value']

        if QID_HUMAN in instance_qids and entity.get_enwiki_title() is not None and len(entity.get_enwiki_title().strip()) > 0:
            en_title = entity_dict['labels']['en']['value'].strip() if 'en' in entity_dict['labels'] else entity.get_enwiki_title()
            l_rows = parse_variants(en_title, entity_dict)
            label_out_f.write(l_rows)
            gender, birth = parse_attributes(entity)
            stats_out_f.write(f"{entity_dict['id']}\t{gender}\t{birth}\t{';'.join(entity_dict['labels'].keys())}\n")
            n_entities += 1

            if n_entities % 10000 == 0:
                print('#Entities parsed:', n_entities, '\tFailures:', n_failures)
    except Exception as e:
        n_failures += 1
        print(e)
        continue
        
    if n_failures > 10000: 
        print('Too many failures, stopping.')
        break

print('Total number of entities:', n_entities, '\tFailures:', n_failures)
label_out_f.close()