import pandas as pd
from loguru import logger


def filter_on_start_end_dates(df):
    return df[
        (df.note_date >= df.start_date)
        & (df.note_date <= df.end_date)
        ]


def remove_index_dates(df):
    return df[df.pat_enc_csn_id != df.index_pat_enc_csn_id]


def attach_results_to_correct_encounter(ds1, ds2, on='studyid', skip_date_filter=False):
    df = pd.merge(ds1, ds2, on=on)
    if skip_date_filter:
        return df
    df['start_date'] = pd.to_datetime(df.start_date)  # index_date - 1
    df['end_date'] = pd.to_datetime(df.end_date)  # index_date - 365
    df['note_date'] = pd.to_datetime(df.note_date)
    return df[
        (df.note_date >= df.start_date)
        & (df.note_date <= df.end_date)
        ]


def build_nlp_positive_table(historical_res_df2, historical_lmt_df):
    nlp_positive = historical_res_df2.groupby(['studyid', 'index_pat_enc_csn_id'])['note_date'].nunique().reset_index()
    nlp_positive = pd.merge(nlp_positive,
                            historical_res_df2['index_pat_enc_csn_id'].drop_duplicates(), on='index_pat_enc_csn_id')
    nlp_positive = nlp_positive.fillna(0)
    del nlp_positive['studyid']
    nlp_positive = pd.merge(nlp_positive, historical_lmt_df[['index_pat_enc_csn_id']].drop_duplicates(),
                            on='index_pat_enc_csn_id', how='outer')
    nlp_positive.columns = ['pat_enc_csn_id', 'note_count']
    nlp_positive['note_count'] = nlp_positive['note_count'].fillna(0).apply(int)
    logger.info('Data on nlp_positive table:')
    logger.info(f' * Number of records: {nlp_positive.shape[0]} (expected: ?)')
    logger.info(f' * Number of unique pat_enc_csn_ids: {nlp_positive.pat_enc_csn_id.nunique()}')
    logger.info(f' * Total number of notes: {nlp_positive.note_count.sum()}')
    return nlp_positive


def build_nlp_model_table(historical_ct_df2):
    nlp_model = historical_ct_df2[
        historical_ct_df2['concept_term'].notnull()
    ][['index_pat_enc_csn_id', 'note_date', 'concept_term']]
    nlp_model = nlp_model.drop_duplicates()
    nlp_model.columns = ['index_pat_enc_csn_id', 'note_date', 'concept_term']
    logger.info('Data on nlp_model table:')
    logger.info(f' * Number of records: {nlp_model.shape[0]}')
    logger.info(f' * Number of unique index pat_enc_csn_ids: {nlp_model.index_pat_enc_csn_id.nunique()}')
    return nlp_model


def build_nlp_index_table(index_ct_df2):
    nlp_index = index_ct_df2[
        index_ct_df2['concept_term'].notnull()
    ][['pat_enc_csn_id', 'note_date', 'concept_term', 'capture']]
    nlp_index.columns = ['pat_enc_csn_id', 'note_date', 'concept_term', 'text_string']
    nlp_index = nlp_index.drop_duplicates()
    logger.info('Data on nlp_index table:')
    logger.info(f' * Number of records: {nlp_index.shape[0]}')
    logger.info(f' * Number of unique pat_enc_csn_ids: {nlp_index.pat_enc_csn_id.nunique()}')
    return nlp_index


def build_nlp_regex_table(index_ct_df, historical_ct_df):
    """Build debugging output showing the hits of each regular expression with context."""
    index_ct_df['is_index'] = 1
    historical_ct_df['is_index'] = 0
    nlp_regex = pd.concat((index_ct_df, historical_ct_df))
    logger.info('Data on nlp_regex table:')
    logger.info(f' * Number of records: {nlp_regex.shape[0]}')
    return nlp_regex
