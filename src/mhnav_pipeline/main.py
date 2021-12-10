"""

"""
import datetime
import pathlib
import sqlalchemy as sa
from loguru import logger

from mhnav_pipeline.bratdb_utils import apply_regex_and_merge
from mhnav_pipeline.build_datasets import build_nlp_positive_table, build_nlp_model_table, build_nlp_index_table, \
    attach_results_to_correct_encounter, remove_index_dates
from mhnav_pipeline.read_data import read_dataset


def build_datasets(index_dataset, historical_dataset, regex_file, *,
                   in_connection_string=None, outpath=None, out_connection_string=None,
                   output_to_csv=True):
    now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    engine_in = sa.create_engine(in_connection_string) if in_connection_string else None
    engine_out = sa.create_engine(out_connection_string) if out_connection_string else None
    outpath = pathlib.Path(outpath) / now if outpath else pathlib.Path('.')
    outpath.mkdir(exist_ok=True, parents=True)

    # load data
    logger.info('Loading data.')
    index_df = read_dataset(index_dataset, engine=engine_in)
    historical_df = read_dataset(historical_dataset, 'index_pat_enc_csn_id', engine=engine_in)

    # process index data
    logger.info('Processing index data with bratdb-apply.')
    _, index_ct_df = apply_regex_and_merge(index_df, regex_file)
    retained_enc_ids = index_ct_df.pat_enc_csn_id.unique()
    index_lmt_df = index_df[index_df.pat_enc_csn_id.isin(retained_enc_ids)]

    # process historical data
    logger.info('Processing historical data with bratdb-apply.')
    historical_lmt_df = historical_df[historical_df.index_pat_enc_csn_id.isin(retained_enc_ids)]
    historical_res_df, historical_ct_df = apply_regex_and_merge(historical_lmt_df, regex_file)

    # merge with metadata
    logger.info('Merging with metadata.')
    historical_res_df2 = attach_results_to_correct_encounter(
        historical_lmt_df, historical_res_df,
        on=['studyid', 'pat_enc_csn_id', 'index_pat_enc_csn_id', 'start_date', 'end_date', 'note_date'],
        skip_date_filter=True
    )
    historical_ct_df2 = remove_index_dates(attach_results_to_correct_encounter(
        historical_lmt_df, historical_ct_df,
        on=['studyid', 'start_date', 'end_date', 'pat_enc_csn_id', 'index_pat_enc_csn_id', 'note_date'],
        skip_date_filter=True
    ))
    index_ct_df2 = attach_results_to_correct_encounter(
        index_lmt_df, index_ct_df,
        on=['end_date', 'note_date', 'pat_enc_csn_id', 'start_date', 'studyid'],
        skip_date_filter=True
    )

    # produce output
    logger.info('Building final tables.')
    nlp_positive = build_nlp_positive_table(historical_res_df2, historical_lmt_df)
    nlp_model = build_nlp_model_table(historical_ct_df2)
    nlp_index = build_nlp_index_table(index_ct_df2)

    # output data
    if output_to_csv:
        logger.info(f'Outputting tables to CSV: {outpath}.')
        nlp_positive.to_csv(outpath / f'nlp_positive_{now}.csv', index=False)
        nlp_model.to_csv(outpath / f'nlp_model_{now}.csv', index=False)
        nlp_index.to_csv(outpath / f'nlp_index_{now}.csv', index=False)

    # output sql
    if engine_out:
        logger.info(f'Outputting tables to SQL Server: {out_connection_string}.')
        nlp_positive.to_sql(f'nlp_positive_{now}', con=engine_out, index=False)
        nlp_model.to_sql(f'nlp_model_{now}', con=engine_out, index=False)
        nlp_index.to_sql(f'nlp_index_{now}', con=engine_out, index=False)

    return nlp_positive, nlp_model, nlp_index


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
    build_datasets(**vars(parser.parse_args()))


if __name__ == '__main__':
    run()
