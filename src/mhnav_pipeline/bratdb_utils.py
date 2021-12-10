import pandas as pd
from bratdb.funcs.apply import apply_regex_to_df


def contains(lst, s):
    for el in lst:
        if el in s:
            return True


def convert_terms(df):
    res = []
    for r in df.itertuples():
        d = r._asdict()
        if r.concept in {'BEHAV_SYMPT', 'ENV_STRESS', 'MH_REFERRAL'}:
            d['concept_term'] = r.concept
            res.append(d)
        if 'depres' in r.term:
            d['concept_term'] = 'depression'
            res.append(d)
        if contains(['academ', 'grade', 'school'], r.term):
            d['concept_term'] = 'academic'
            res.append(d)
        if contains(['add', 'adhd', 'attention'], r.term):
            d['concept_term'] = 'adhd'
            res.append(d)
        if 'anger' in r.term:
            d['concept_term'] = 'anger'
            res.append(d)
        if 'anx' in r.term:
            d['concept_term'] = 'anxiety'
            res.append(d)
        if 'bully' in r.term:
            d['concept_term'] = 'bully'
            res.append(d)
        if contains(['defia', 'oppositional'], r.term):
            d['concept_term'] = 'defiant'
            res.append(d)
        if contains(['drug', 'substance'], r.term):
            d['concept_term'] = 'drug'
            res.append(d)
        if 'meds' in r.term:
            d['concept_term'] = 'meds'
            res.append(d)
        if 'suic' in r.term:
            d['concept_term'] = 'suicide'
            res.append(d)
    return pd.DataFrame(res)


def apply_regex_and_merge(df, regex_file):
    results_df = pd.DataFrame(
        apply_regex_to_df(regex_file, df),
        columns=['id', 'concept', 'term', 'capture']
    )
    res_df = pd.merge(df, results_df, left_index=True, right_on='id', how='inner')
    ct_df = convert_terms(res_df)
    return res_df.drop_duplicates(), ct_df.drop_duplicates()
