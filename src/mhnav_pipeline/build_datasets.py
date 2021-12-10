import pandas as pd


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
    nlp_positive.columns = ['pat_enc_csn_id', 'note_count']
    nlp_positive = pd.merge(nlp_positive, historical_lmt_df[['pat_enc_csn_id']].drop_duplicates(),
                            on='pat_enc_csn_id', how='outer')
    nlp_positive['note_count'] = nlp_positive['note_count'].fillna(0).apply(int)
    nlp_positive.shape[0], nlp_positive['pat_enc_csn_id'].nunique(), nlp_positive.note_count.sum()
    return nlp_positive


def build_nlp_model_table(historical_ct_df2):
    nlp_model = historical_ct_df2[
        historical_ct_df2['concept_term'].notnull()
    ][['index_pat_enc_csn_id', 'note_date', 'concept_term']]
    nlp_model = nlp_model.drop_duplicates()
    nlp_model.columns = ['index_pat_enc_csn_id', 'note_date', 'concept_term']
    nlp_model.shape[0], nlp_model['index_pat_enc_csn_id'].nunique()
    return nlp_model


def build_nlp_index_table(index_ct_df2):
    nlp_index = index_ct_df2[
        index_ct_df2['concept_term'].notnull()
    ][['pat_enc_csn_id', 'note_date', 'concept_term', 'capture']]
    nlp_index.columns = ['pat_enc_csn_id', 'note_date', 'concept_term', 'text_string']
    nlp_index = nlp_index.drop_duplicates()
    nlp_index.shape[0], nlp_index['pat_enc_csn_id'].nunique()
    return nlp_index
