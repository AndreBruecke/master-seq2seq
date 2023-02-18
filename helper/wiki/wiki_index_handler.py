"""
Splitting and searching of wikipedia multistream index files.
"""

import pandas as pd
import os
import re

from matchers import *
from typing import Callable
from unidecode import unidecode


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


def search_chunks(index_file_path: str, matches: Callable):
    byte_offsets = []
    with open(index_file_path, encoding='utf-8') as ix_f:
        for i, line in enumerate(ix_f):
            vals = line.split(':', 2)
            if matches(vals[2]) and int(vals[0]) not in byte_offsets:
                byte_offsets.append(int(vals[0]))
    chunks = [[byte_offsets[0]]]
    for i, next_offset in enumerate(byte_offsets[1:]):
        if next_offset - chunks[-1][-1] > 100000000:   # More than 100 MB between offsets
            chunks.append([next_offset])
        else:
            chunks[-1].append(next_offset)
    print('\n'.join([str(c) for c in chunks]))
    # df = pd.read_csv(index_file_path, sep=':', encoding='utf-8')
    # print(df.head())


# split_index_file('D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/enwiki-20211020-pages-articles-multistream-index.txt', 'D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/')
write_unique_offsets('D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/enwiki-20211020-pages-articles-multistream-index.txt', 'D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/')

# search_chunks('D:/HDa/Thesis/Repos/master-seq2seq/large_data/wiki_db_en/enwiki-20211020-index__L.txt', match_alphabetical_film_categories)

