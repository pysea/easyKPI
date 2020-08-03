import os, glob, sys


dic_word = {
    'moitié'    :   '50%',
    'quart'     :   '25%',
    'tier'      :   '33%',
    'un'        :   '1',
    'deux'      :   '2',
    'trois'     :   '3',
    'quatre': '4',
    'cinq': '5',
    'six': '6',
    'sept': '7',
    'huit': '8',
    'neuf': '9',
    'dix': '10',
    'onze': '11',
    'douze': '12',
    'treize': '13',
    'quatorze': '14',
    'quinze': '15',
    'seize': '16',
    'dix-sept': '17',
    'dix-huit': '18',
    'dix-neuf': '19',
    'dix-neuf': '20'

}


def ppSent(sent):
    sent = sent.replace('\n', ' ') \
        .replace('\n\n', ' ') \
        .replace('\n\n\n', ' ')
    sent = sent.replace('\t', ' ') \
        .replace('\t\t', ' ') \
        .replace('\t\t\t', ' ')
    sent = sent.replace('\t', ' ') \
        .replace('\t\t', ' ') \
        .replace('\t\t\t', ' ')
    sent = sent.replace('\u2019', ' ')
    sent = sent.replace('\u2011', ' ')
    sent = sent.replace('\u25c6', ' ')
    sent = sent.replace('\ufb01', ' ')

    return sent