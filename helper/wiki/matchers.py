import re


def match_alphabetical_film_categories(title: str):
    if not title.startswith('List of films'):
        return False
    return re.match(r'List of films: ([A-Z](-[A-Z])*|numbers)', title)

def match_list_of_people_categories(title: str):
    if 'Lists of' not in title:
        return False
    return any([term in title.lower() for term in ['people', ' male ', ' female ', ' men', ' women', ' actors', ' actresses', ' politicians']])

def match_list_of_people_pages(title: str):
    if not title.startswith('List of'):
        return False
    # TODO