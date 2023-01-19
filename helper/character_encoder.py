import numpy as np

from typing import Iterable

charsets = {
    'simple': sorted(list(' \',-.abcdefghijklmnopqrstuvwxyz\t\n'))
}


class DictionaryCharacterEncoder:

    def __init__(self, charset='simple'):
        self.charset = charsets[charset] + ['§']
        self.char_index = dict([(char, i) for i, char in enumerate(self.charset)])

    def encode(self, texts: Iterable, insert_markers=False, shift=False):
        max_seq_length = max([len(t) for t in texts]) + (2 if insert_markers else 0)
        encoded_data = np.zeros((len(texts), max_seq_length, len(self.charset)), dtype="float32")

        for i, text in enumerate(texts):
            if insert_markers:
                text = '\t' + text + '\n'
            for j, char in enumerate(text):
                if not shift or j > 0:
                    encoded_data[i, j - (1 if shift else 0), self.char_index[char]] = 1.0
            encoded_data[i, j + (0 if shift else 1):, self.char_index[' ']] = 1.0

    def decode(self):
        pass


DictionaryCharacterEncoder()
