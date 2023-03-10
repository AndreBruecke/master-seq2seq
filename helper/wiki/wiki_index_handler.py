"""
Splitting and searching of wikipedia multistream index files.
"""

import bz2
import numpy as np
import pandas as pd
import os
import re
import shutil
import xml.etree.ElementTree as ET

from matchers import *
from typing import Callable
from unidecode import unidecode

from xml.etree.ElementTree import Element


def split_index_file(index_file_path: str, out_folder: str, skip_single_character_titles: bool = True):
    file_name = re.findall(r'[a-z]{2}wiki-\d{8}', index_file_path)[0]
    print(file_name)
    with open(index_file_path, encoding='utf-8') as ix_f:
        prev = ''
        current_out_f = None
        for i, line in enumerate(ix_f):
            title = line.split(':', 2)[2]
            if len(title) < (2 if skip_single_character_titles else 1):
                continue
            start_char = unidecode(str(title[0]))
            start_char = start_char if re.fullmatch(r'[A-Z]', start_char) else 'special'
            if prev != start_char:
                if current_out_f is not None: current_out_f.close()
                out_file_name = f'{file_name}-index__{start_char}.txt'
                current_out_f = open(os.path.join(out_folder, out_file_name), 'a', encoding='utf-8')
                prev = start_char
            current_out_f.write(line)


def write_unique_offsets(index_file_path: str, out_folder: str):
    unique_offsets = []
    with open(index_file_path, encoding='utf-8') as ix_f:
        for i, line in enumerate(ix_f):
            byte_offset = line.split(':', 2)[0]
            if len(unique_offsets) < 1 or unique_offsets[-1] != byte_offset:
                unique_offsets.append(byte_offset)
    with open(out_folder + 'OFFSETS.txt', 'w', encoding='utf-8') as offset_out_f:
        for offset in unique_offsets:
            offset_out_f.write(offset + '\n')


def search_chunks(index_file_path: str, offset_list_path: str, matches: Callable):
    matching_byte_offsets = []
    offset_df = pd.read_csv(offset_list_path, header=None, names=['start_byte'])
    shifted_df = pd.read_csv(offset_list_path, header=None, names=['next_byte']).shift(-1)
    offset_df = pd.merge(offset_df, shifted_df, how='inner', left_index=True, right_index=True)
    offset_df['next_byte'] = offset_df['next_byte'].fillna(-1).astype(np.int64)

    with open(index_file_path, encoding='utf-8') as ix_f:
        for i, line in enumerate(ix_f):
            vals = line.split(':', 2)
            if matches(vals[2]) and int(vals[0]) not in matching_byte_offsets:
                matching_byte_offsets.append(int(vals[0]))
    matching_chunks = [[matching_byte_offsets[0]]]
    for i, next_offset in enumerate(matching_byte_offsets[1:]):
        # if next_offset - matching_chunks[-1][-1] > 100000000:   # More than 100 MB between offsets
        if next_offset - matching_chunks[-1][-1] > 10000000:   # More than 10 MB between offsets
            matching_chunks.append([next_offset])
        else:
            matching_chunks[-1].append(next_offset)

    chunk_boundaries = []
    for chunk in matching_chunks:
        next_byte = offset_df[offset_df['start_byte'] == chunk[-1]].iloc[0]['next_byte']
        chunk_boundaries.append({ 'start': chunk[0], 'end': next_byte })
    return chunk_boundaries


def save_chunks(mulitstream_path: str, out_folder: str, chunks: dict):
    with open(mulitstream_path, 'rb') as wiki_f:
        for i, chunk in enumerate(chunks):
            wiki_f.seek(chunk['start'])
            data = wiki_f.read(int(chunk['end'] - chunk['start']))
            with open(os.path.join(out_folder, f'{i}-temp.bz2'), 'wb') as temp_out:
                temp_out.write(data)
            with bz2.BZ2File(os.path.join(out_folder, f'{i}-temp.bz2')) as fr, open(os.path.join(out_folder, f'{i}.xml'), 'wb') as fw:
                shutil.copyfileobj(fr, fw)

def merge_chunks(chunk_folder: str, out_path: str, matches: Callable):
    chunk_files = [f for f in os.listdir(chunk_folder) if re.fullmatch(r'\d+\.xml', f)]
    matching_pages = []
    for chunk_file in chunk_files:
        xml = ''
        with open(os.path.join(chunk_folder, chunk_file), 'r', encoding='utf-8') as chunk_f:
            xml = chunk_f.read().strip()
        xml = re.split(r'</?page>', xml)
        for page_content in xml:
            title_elem = re.search(r'<title>(.*?)</title>', page_content)
            if title_elem is None:
                continue
            title = title_elem.group(1).strip()
            if not matches(title):
                continue
            page_content = page_content.replace('<page>', '')
            matching_pages.append('<page>' + page_content.strip() + '</page>')
    with open(out_path, 'w', encoding='utf-8') as merged_f:
        merged_f.write('<root>\n\n')
        merged_f.write('\n\n'.join(matching_pages))
        merged_f.write('\n\n</root>')

    

# split_index_file('D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/enwiki-20211020-pages-articles-multistream-index.txt', 'D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/')
# write_unique_offsets('D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/enwiki-20211020-pages-articles-multistream-index.txt', 'D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/')

# chunk_bounds = search_chunks('T:/MasterData/wikipedia_dump/index_split/enwiki-20211020-index__L.txt', 'T:/MasterData/wikipedia_dump/index_split/enwiki-20211020-index___OFFSETS.txt', match_alphabetical_film_categories)
# save_chunks('T:/MasterData/wikipedia_dump/enwiki-20211020-pages-articles-multistream.xml.bz2', 'T:/MasterData/wikipedia_dump/m_alphabetical_film_categories/', chunk_bounds)

# chunk_bounds = search_chunks('T:/MasterData/wikipedia_dump/index_split/enwiki-20211020-index__C.txt', 'T:/MasterData/wikipedia_dump/index_split/enwiki-20211020-index___OFFSETS.txt', match_list_of_people_categories)
# save_chunks('T:/MasterData/wikipedia_dump/enwiki-20211020-pages-articles-multistream.xml.bz2', 'T:/MasterData/wikipedia_dump/m_list_of_people_categories/', chunk_bounds)
# merge_chunks('T:/MasterData/wikipedia_dump/m_list_of_people_categories/', 'T:/MasterData/wikipedia_dump/m_list_of_people_categories/pages.xml', match_list_of_people_categories)

# chunk_bounds = search_chunks('T:/MasterData/wikipedia_dump/index_split/enwiki-20211020-index__L.txt', 'T:/MasterData/wikipedia_dump/index_split/enwiki-20211020-index___OFFSETS.txt', match_list_of_people_pages)
# save_chunks('T:/MasterData/wikipedia_dump/enwiki-20211020-pages-articles-multistream.xml.bz2', 'T:/MasterData/wikipedia_dump/m_list_of_people_pages/', chunk_bounds)
# merge_chunks('T:/MasterData/wikipedia_dump/m_list_of_people_pages/', 'T:/MasterData/wikipedia_dump/m_list_of_people_pages/pages.xml', match_list_of_people_pages)