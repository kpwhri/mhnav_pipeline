"""
This file is for local modifications/changes to exclude boilerplate, etc.

Please copy-paste the file and give it the name `cleaning.py`.
"""
import re

import pandas as pd
from loguru import logger

from mhnav_pipeline.local.tracking import add_replacement


def replace(text, regex_str):
    regex = re.compile(regex_str.replace(' ', r'\W+'), re.I)
    new_text = regex.sub(' ', text)
    if text != new_text:
        add_replacement(regex_str)
    return new_text


def clean_text(text):
    """Remove boilerplate, etc."""
    if pd.isnull(text):
        return ''
    if should_be_excluded(text):
        return ''
    text = '  '.join(text.split('\n'))
    # example of text-replacement process
    # text = replace(text, 'smoking tobacco use: (?:never smoker|not assessed)')
    return text


def should_be_excluded(text):
    """
    Indications that the entire note should be excluded from consideration.
    :param text:
    :return:
    """
    start_text = text[:100].lower()
    phrases = []  # add phrases which if found in first 100 characters, the entire note should be excluded
    for phrase in phrases:
        if phrase in start_text:
            add_replacement(phrase)
            return True
    return False
