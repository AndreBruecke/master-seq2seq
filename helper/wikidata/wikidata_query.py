import os
import unicodedata as ud

from qwikidata.entity import WikidataItem
from qwikidata.json_dump import WikidataJsonDump

PROP_INSTANCE_OF = 'P31'
PROP_GENDER = 'P21'
PROP_BIRTH = 'P569'

QID_HUMAN = 'Q5'

latin_letters = {}
def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr) for uchr in unistr if uchr.isalpha())


def parse_human_item(en_label, entity_dict):
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


dump_path = 'T:/MasterData/wikidata_dump/wikidata-20220103-all.json.gz'
out_folder = 'T:/MasterData/wikidata_dump/'
reader = WikidataJsonDump(dump_path)

label_out_f = open(os.path.join(out_folder, 'human_labels.tsv'), 'w', encoding='utf-8')
label_out_f.write('id\tlang\tlabel\n')
# alias_out_f = open(os.path.join(out_folder, 'human_aliases.tsv'), 'w', encoding='utf-8')
# alias_out_f.write('id\tlang\taliases\n')

humans = []
for i, entity_dict in enumerate(reader):
    try:
        if entity_dict['type'] != 'item': 
            continue

        entity = WikidataItem(entity_dict)
        claim_group = entity.get_truthy_claim_group(PROP_INSTANCE_OF)
        instance_qids = [
            claim.mainsnak.datavalue.value['id']
            for claim in claim_group
            if claim.mainsnak.snaktype == 'value'
        ]

        if QID_HUMAN in instance_qids and entity.get_enwiki_title() is not None:
            humans.append(entity)
            l_rows = parse_human_item(entity_dict['labels']['en']['value'].strip(), entity_dict)
            label_out_f.write(l_rows)
    except Exception as e:
        print(i, e)
        continue

    if len(humans) > 500: break

label_out_f.close()