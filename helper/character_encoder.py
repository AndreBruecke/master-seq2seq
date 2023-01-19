
charsets = {
    'simple': sorted(list(' \',-.abcdefghijklmnopqrstuvwxyz\t\n'))
}


class DictionaryCharacterEncoder:

    def __init__(self, charset='simple'):
        self.charset = charsets[charset] + ['§']
        print(self.charset)

    def encode(self, text, insert_markers=False):
        pass

    def decode(self):
        pass


DictionaryCharacterEncoder()
