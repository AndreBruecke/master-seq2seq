import os
import unicodedata as ud

from qwikidata.entity import WikidataItem
from qwikidata.json_dump import WikidataJsonDump

PROP_INSTANCE_OF = 'P31'
PROP_COUNTRY = 'P17'

# Human properties
PROP_GENDER = 'P21'
PROP_BIRTH = 'P569'
PROP_GIVEN_NAME = 'P735'
PROP_FAMILY_NAME = 'P734'

# Organization properties
PROP_SHORT_NAME = 'P1813'
PROP_NATIVE_LABEL = 'P1705'

QID_HUMAN = 'Q5'

def is_orga(instance_qids) -> bool:
    qids_to_check = ['Q891723', 'Q43229', 'Q783794', 'Q352450', 'Q157031', 'Q79913', 'Q167037', 'Q6881511', 'Q11032', 'Q8869', 'Q18127', 'Q11707', 'Q22687', 'Q216107', 'Q4830453', 'Q206361', 'Q380342', 'Q380962', 'Q180846', 'Q161726', 'Q219577', 'Q76470', 'Q134161', 
                     'Q141683', 'Q149789', 'Q165758', 'Q166280', 'Q207320', 'Q187047', 'Q149985', 'Q155076', 'Q170161', 'Q157963', 'Q157165', 'Q213441', 'Q162157', 'Q129238', 'Q216931', 'Q217107', 'Q197952', 'Q163740', 'Q251927', 'Q941185', 'Q646164', 
                     'Q261428', 'Q988108', 'Q967140', 'Q672386', 'Q294163', 'Q327333', 'Q319845', 'Q730038', 'Q728646', 'Q745109', 'Q745877', 'Q748019', 'Q765517', 'Q459195', 'Q484652', 'Q829080', 'Q563787', 'Q848507', 'Q567521', 'Q614084', 'Q865588', 
                     'Q33685', 'Q27493', 'Q1156831', 'Q27686']
    for qid in qids_to_check:
        if qid in instance_qids: return True
    return False

def is_geo(instance_qids) -> bool:
    qids_to_check = ['Q515', 'Q532', 'Q4022', 'Q1549591', 'Q486972', 'Q7930989', 'Q15284', 'Q23397', 'Q4421', 'Q8502', 'Q46831', 'Q23442', 'Q34876', 'Q39594', 'Q40080', 'Q46169', 'Q82794', 'Q93352', 'Q3957', 'Q47521', 'Q54050', 'Q12280', 'Q12284', 'Q15324']
    for qid in qids_to_check:
        if qid in instance_qids: return True
    return False


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

def parse_human_attributes(entity: WikidataItem):
    claim_group = entity.get_truthy_claim_group(PROP_GENDER)
    gender_qids = [claim.mainsnak.datavalue.value['id'] for claim in claim_group if claim.mainsnak.snaktype == 'value']
    gender = '' if len(gender_qids) < 1 else gender_qids[0]
    gender = gender.replace('Q6581097', 'male').replace('Q6581072', 'female')

    claim_group = entity.get_truthy_claim_group(PROP_BIRTH)
    date_values = [claim.mainsnak.datavalue.value['time'] for claim in claim_group if claim.mainsnak.snaktype == 'value']
    date = '' if len(date_values) < 1 else date_values[0]
    date = date.split('T')[0]
    return gender, date

def parse_orga_attributes(entity: WikidataItem):
    if 'descriptions' in entity._entity_dict and 'en' in entity._entity_dict['descriptions']:
        return entity._entity_dict['descriptions']['en']['value'].strip()
    return None

def parse_geo_attributes(entity: WikidataItem):
    if 'descriptions' in entity._entity_dict and 'en' in entity._entity_dict['descriptions']:
        return entity._entity_dict['descriptions']['en']['value'].strip()
    return None


dump_path = 'T:/MasterData/wikidata_dump/wikidata-20220103-all.json.gz'
out_folder = 'T:/MasterData/wikidata_dump/'
reader = WikidataJsonDump(dump_path)

label_out_f = open(os.path.join(out_folder, 'organization_labels_2.tsv'), 'w', encoding='utf-8')
label_out_f.write('id\tlang\tlabel\n')
stats_out_f = open(os.path.join(out_folder, 'organization_stats_2.tsv'), 'w', encoding='utf-8')
stats_out_f.write('id\tdescription\titem_languages\n')

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

        # if QID_HUMAN in instance_qids and entity.get_enwiki_title() is not None and len(entity.get_enwiki_title().strip()) > 0:
        if is_orga(instance_qids) and entity.get_enwiki_title() is not None and len(entity.get_enwiki_title().strip()) > 0:
            en_title = entity_dict['labels']['en']['value'].strip() if 'en' in entity_dict['labels'] else entity.get_enwiki_title()
            l_rows = parse_variants(en_title, entity_dict)
            label_out_f.write(l_rows)
            # gender, birth = parse_human_attributes(entity)
            desc = parse_orga_attributes(entity)
            stats_out_f.write(f"{entity_dict['id']}\t{desc}\t{';'.join(entity_dict['labels'].keys())}\n")
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