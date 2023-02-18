import re


def match_alphabetical_film_categories(title: str):
    if not title.startswith('List of films'):
        return False
    return re.match(r'List of films: ([A-Z](-[A-Z])*|numbers)', title)
