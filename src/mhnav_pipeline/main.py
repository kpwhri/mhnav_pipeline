"""

"""
import datetime
import pathlib

import pandas as pd
import sqlalchemy as sa
from loguru import logger

from mhnav_pipeline.bratdb_utils import apply_regex_and_merge
from mhnav_pipeline.build_datasets import build_nlp_positive_table, build_nlp_model_table, build_nlp_index_table, \
    attach_results_to_correct_encounter, remove_index_dates, build_nlp_regex_table
from mhnav_pipeline.local.cleaning import clean_text
from mhnav_pipeline.local.tracking import log_and_reset_replacements
from mhnav_pipeline.read_data import read_dataset


def print_dataset(dataset):
    if isinstance(dataset, pd.DataFrame):
        return 'DataFrame'
    elif isinstance(dataset, (str, pathlib.Path)):
        return dataset
    else:
        logger.info(f'Not sure how to format: {type(dataset)}')
        return dataset


def build_datasets(index_dataset, historical_dataset, regex_file, *,
                   in_connection_string=None, outpath=None, out_connection_string=None,
                   output_to_csv=True,
                   nlp_positive_tablename=None,
                   nlp_model_tablename=None,
                   nlp_index_tablename=None,
                   nlp_regex_tablename=None,
                   overwrite_existing=False,
                   include_context=0):
    """
    Build datasets of text and then run regular expressions with bratdb-apply on the text. Retain
        instances that are useful for the Mental Health Navigator model and output those as CSV/db.

    :param index_dataset:
    :param historical_dataset:
    :param regex_file:
    :param in_connection_string:
    :param outpath:
    :param out_connection_string:
    :param output_to_csv:
    :param nlp_positive_tablename: (optional) specify exact name for table
    :param nlp_model_tablename: (optional) specify exact name for table
    :param nlp_index_tablename: (optional) specify exact name for table
    :param nlp_regex_tablename: (optional) specify exact name for table
    :param overwrite_existing: overwrite existing tables
    :param include_context: specify context to include for debugging
    :return:
    """
    logger.info(f'Beginning process of building datasets for Mental Health Navigator.')
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    engine_in = sa.create_engine(in_connection_string) if in_connection_string else None
    engine_out = sa.create_engine(out_connection_string) if out_connection_string else None
    outpath = pathlib.Path(outpath) / now if outpath else pathlib.Path('.')
    outpath.mkdir(exist_ok=True, parents=True)

    # load data
    logger.info(f'Loading index data from {print_dataset(index_dataset)}.')
    index_df = read_dataset(index_dataset, engine=engine_in)
    # columns: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid', 'end_date', 'note_text']
    logger.info(f'Loaded {index_df.shape[0]} records for index dataset.')
    logger.info(f'Loading historical data from {print_dataset(historical_dataset)}.')
    historical_df = read_dataset(historical_dataset, 'index_pat_enc_csn_id', engine=engine_in)
    # columns: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid', 'index_pat_enc_csn_id', 'end_date', 'note_text']
    logger.info(f'Loaded {index_df.shape[0]} records for historical dataset.')

    # clean text
    index_df['note_text'] = index_df['note_text'].apply(clean_text)
    log_and_reset_replacements()
    historical_df['note_text'] = historical_df['note_text'].apply(clean_text)
    log_and_reset_replacements()

    # process index data
    logger.info('Processing index data with bratdb-apply.')
    _, index_ct_df = apply_regex_and_merge(index_df, regex_file, include_context=include_context)
    # columns: ['Index', 'start_date', 'pat_enc_csn_id', 'note_date', 'studyid',
    #        'end_date', 'note_text', 'id', 'concept', 'term', 'capture', 'concept_term'] +
    #        ['precontext', 'postcontext'] if include_context > 0
    retained_enc_ids = index_ct_df.pat_enc_csn_id.unique()
    index_lmt_df = index_df[index_df.pat_enc_csn_id.isin(retained_enc_ids)]
    # columns: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid', 'end_date', 'note_text']

    # process historical data
    logger.info('Processing historical data with bratdb-apply.')
    historical_lmt_df = historical_df[historical_df.index_pat_enc_csn_id.isin(retained_enc_ids)]
    historical_res_df, historical_ct_df = apply_regex_and_merge(
        historical_lmt_df, regex_file, include_context=include_context
    )
    # columns [lmt]: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid', 'index_pat_enc_csn_id', 'end_date',
    #        'note_text']
    # columns [res]: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid',
    #        'index_pat_enc_csn_id', 'end_date', 'note_text', 'id', 'concept',
    #        'term', 'capture'] + ['precontext', 'postcontext'] if include_context > 0
    # columns [ct]: ['Index', 'start_date', 'pat_enc_csn_id', 'note_date', 'studyid', 'term', 'capture',
    #        'index_pat_enc_csn_id', 'end_date', 'note_text', 'id', 'concept', 'concept_term'] +
    #        ['precontext', 'postcontext'] if include_context > 0

    # merge with metadata
    logger.info('Merging results of bratdb-apply with metadata.')
    historical_res_df2 = attach_results_to_correct_encounter(
        historical_lmt_df, historical_res_df,
        on=['studyid', 'pat_enc_csn_id', 'index_pat_enc_csn_id', 'start_date', 'end_date', 'note_date'],
        skip_date_filter=True
    )
    # columns: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid',
    #        'index_pat_enc_csn_id', 'end_date', 'note_text_x', 'note_text_y', 'id',
    #        'concept', 'term', 'capture'] +? ['precontext', 'postcontext']
    historical_ct_df2 = remove_index_dates(attach_results_to_correct_encounter(
        historical_lmt_df, historical_ct_df,
        on=['studyid', 'start_date', 'end_date', 'pat_enc_csn_id', 'index_pat_enc_csn_id', 'note_date'],
        skip_date_filter=True
    ))
    # columns: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid',
    #        'index_pat_enc_csn_id', 'end_date', 'note_text_x', 'Index',
    #        'note_text_y', 'id', 'concept', 'term', 'capture', 'concept_term'] +?
    #        ['precontext', 'postcontext']
    index_ct_df2 = attach_results_to_correct_encounter(
        index_lmt_df, index_ct_df,
        on=['end_date', 'note_date', 'pat_enc_csn_id', 'start_date', 'studyid'],
        skip_date_filter=True
    )
    # columns: ['start_date', 'pat_enc_csn_id', 'note_date', 'studyid', 'end_date',
    #        'note_text_x', 'Index', 'note_text_y', 'id', 'concept', 'term',
    #        'capture', 'concept_term'] +? ['precontext', 'postcontext']

    # produce output
    logger.info('Building final tables.')
    nlp_positive = build_nlp_positive_table(historical_res_df2, historical_lmt_df)
    nlp_model = build_nlp_model_table(historical_ct_df2)
    nlp_index = build_nlp_index_table(index_ct_df2)
    if include_context:
        nlp_regex = build_nlp_regex_table(index_ct_df, historical_ct_df)
    else:
        nlp_regex = None

    # output data
    if output_to_csv:
        logger.info(f'Outputting tables to CSV: {outpath}.')
        nlp_positive.to_csv(outpath / f'nlp_positive_{now}.csv', index=False)
        nlp_model.to_csv(outpath / f'nlp_model_{now}.csv', index=False)
        nlp_index.to_csv(outpath / f'nlp_index_{now}.csv', index=False)
        if include_context:  # nlp_regex exists if include_context > 0
            nlp_regex.to_csv(outpath / f'nlp_regex_{now}.csv', index=False)

    # output sql
    if engine_out:
        logger.info(f'Outputting tables to database: {out_connection_string}.')
        nlp_positive_tablename = nlp_positive_tablename if nlp_positive_tablename else f'nlp_positive_{now}'
        nlp_model_tablename = nlp_model_tablename if nlp_model_tablename else f'nlp_model_{now}'
        nlp_index_tablename = nlp_index_tablename if nlp_index_tablename else f'nlp_index_{now}'
        nlp_positive.to_sql(nlp_positive_tablename, con=engine_out, index=False,
                            if_exists='replace' if overwrite_existing else 'fail')
        nlp_model.to_sql(nlp_model_tablename, con=engine_out, index=False,
                         if_exists='replace' if overwrite_existing else 'fail')
        nlp_index.to_sql(nlp_index_tablename, con=engine_out, index=False,
                         if_exists='replace' if overwrite_existing else 'fail')
        if include_context:  # nlp_regex exists if include_context > 0
            nlp_regex.to_sql(nlp_regex_tablename, con=engine_out, index=False,
                             if_exists='replace' if overwrite_existing else 'fail')

    logger.info(f'Process completed.')
    return nlp_positive, nlp_model, nlp_index, nlp_regex


def run():
    import argparse

    parser = argparse.ArgumentParser(fromfile_prefix_chars='@!')
    parser.add_argument('-i', '--index-dataset', dest='index_dataset', required=True,
                        help='Dataset from the target encounters information.'
                             ' Must have the following columns: pat_enc_csn_id,'
                             ' studyid, note_date, note_text, start_date, end_date.')
    parser.add_argument('-s', '--historical-dataset', dest='historical_dataset', required=True,
                        help='Dataset from one day before to 365 days before the encounter date.'
                             ' Must have the following columns: index_pat_enc_csn_id, pat_enc_csn_id,'
                             ' studyid, note_date, note_text, start_date, end_date.'
                             ' May be dataframe, CSV file, or table name.')
    parser.add_argument('-r', '--regex-file', dest='regex_file', required=True, type=pathlib.Path,
                        help='Full path to regex file for running bratdb-apply.')
    parser.add_argument('--in-connection-string', dest='in_connection_string',
                        help='SQL Alchemy-style connection string to retrieve index and historical datasets.'
                             ' --index-dataset and --historical-dataset must be set to tablenames.')
    parser.add_argument('--out-connection-string', dest='out_connection_string',
                        help='SQL Alchemy-style connection string to output result datasets.')
    parser.add_argument('--dont-output-to-csv', dest='output_to_csv', default=True, action='store_false',
                        help='Skip writing data to file. (If using database only, or want to just return'
                             ' result dataframes.')
    parser.add_argument('--include-context', dest='include_context', required=False, default=0, type=int,
                        help='Include debugging context for regular expressions on a note-by-note level.')
    build_datasets(**vars(parser.parse_args()))


if __name__ == '__main__':
    run()
