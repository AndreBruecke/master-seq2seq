import numpy as np

from typing import Iterable

charsets = {
    'simple': sorted(list(' \',-.abcdefghijklmnopqrstuvwxyz\t\n'))
}


class DictionaryCharacterEncoder:
    """_Provides reproducible character-level vectorization._
    """

    def __init__(self, charset='simple', max_seq_length=100):
        self.charset = charsets[charset] + ['§']  # Adds a special symbol, to which all unknown characters are mapped to during encoding.
        self.char_index = dict([(char, i) for i, char in enumerate(self.charset)])  # Dictionary mapping characters to their indices.
        self.inverse_char_index = dict((i, char) for char, i in self.char_index.items())
        self.max_seq_length = max_seq_length

    def encode(self, sequences: Iterable, insert_markers=False, shift=False):
        """_Encodes a collection of sequences using character-level one-hot encoding._

        Arguments:
            sequences -- _Iterable input sequences._
            insert_markers -- _Whether sequences should to be wrapped with start/end markers._ (default: {False})
            shift -- _Whether sequences should be shifted by one character._ (default: {False})

        Returns:
            _Numpy array of shape (NUMBER_OF_SEQUENCES, MAX_SEQUENCE_LENGTH, CHARSET_SIZE)._
        """
        encoded_data = np.zeros((len(sequences), self.max_seq_length, len(self.charset)), dtype="float32")

        for i, seq in enumerate(sequences):
            if insert_markers:
                seq = '\t' + seq + '\n'
            for j, char in enumerate(seq):
                if not shift or j > 0:
                    char_pos = self.char_index[char] if char in self.char_index else self.char_index['§']
                    encoded_data[i, j - (1 if shift else 0), char_pos] = 1.0
            encoded_data[i, j + (0 if shift else 1):, self.char_index[' ']] = 1.0
        
        return encoded_data

    def to_ids(self, sequences: Iterable, insert_markers=False, shift=False):
        """_Encodes a collection of sequences using ids._

        Arguments:
            sequences -- _Iterable input sequences._
            insert_markers -- _Whether sequences should to be wrapped with start/end markers._ (default: {False})
            shift -- _Whether sequences should be shifted by one character._ (default: {False})

        Returns:
            _List of shape (NUMBER_OF_SEQUENCES, MAX_SEQUENCE_LENGTH)._
        """
        encoded_data = []

        for i, seq in enumerate(sequences):
            encoded_data.append([])
            if insert_markers:
                seq = '\t' + seq + '\n'
            for j, char in enumerate(seq):
                if not shift or j > 0:
                    char_pos = self.char_index[char] if char in self.char_index else self.char_index['§']
                    encoded_data[-1].append(char_pos)
        return encoded_data
