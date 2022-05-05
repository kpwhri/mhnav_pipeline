from collections import Counter
from loguru import logger
import pathlib

_REPLACEMENTS = Counter()


def add_replacement(label):
    _REPLACEMENTS[label] += 1


def reset_replacements():
    """Reset replacement count"""
    global _REPLACEMENTS
    _REPLACEMENTS = Counter()


def log_and_reset_replacements():
    logger.info(f'Text cleaning: [replacement: count]')
    for k, v in _REPLACEMENTS.items():
        logger.info(f'* {k}: {v}\n')
    reset_replacements()


def output_and_reset_replacements(outpath: pathlib.Path, label):
    """Output replacements performed to a CSV file"""
    with open(outpath / f'{label}.tsv', 'w') as out:
        out.write(f'pattern\tfrequency\n')
        for k, v in _REPLACEMENTS.items():
            out.write(f'{k}\t{v}\n')
    reset_replacements()


def get_replacements():
    return _REPLACEMENTS.copy()


def get_and_reset_replacements():
    repl = get_replacements()
    reset_replacements()
    return repl
