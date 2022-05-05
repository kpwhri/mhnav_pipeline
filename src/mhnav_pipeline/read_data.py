import pandas as pd
from loguru import logger


def read_dataset(dataset, *extra_cols, engine=None):
    logger.info(f'Loading dataset: {dataset if not isinstance(dataset, pd.DataFrame) else "DataFrame"}')
    if engine:
        return validate_headers(pd.read_sql_table(dataset, con=engine), *extra_cols)
    elif isinstance(dataset, pd.DataFrame):
        return validate_headers(dataset, *extra_cols)
    elif str(dataset).endswith('csv'):
        return validate_headers(pd.read_csv(dataset), *extra_cols)
    elif str(dataset).endswith('sas7bdat'):
        return validate_headers(pd.read_sas(dataset), *extra_cols)
    else:
        e = ValueError(f'Unrecognized filetype: {dataset}')
        logger.exception(e)
        raise e


def validate_headers(df, *extra_cols):
    df.columns = [col.lower() for col in df.columns]
    columns = set(df.columns)
    exp_columns = {'studyid', 'note_text', 'pat_enc_csn_id', 'start_date', 'end_date', 'note_date'}
    if extra_cols:
        exp_columns |= set(extra_cols)
    missing = exp_columns - columns
    if missing:
        e = ValueError(f'Dataset missing columns: {", ".join(missing)}')
        logger.exception(e)
        raise e
    return df[list(exp_columns)].copy()
