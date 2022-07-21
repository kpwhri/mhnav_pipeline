import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='Aggregate and deline notes, and convert from sas7bdat to csv')
parser.add_argument("--groupby", default='note_id', help='column to group notes by')
parser.add_argument("--aggnotecount", default='note_line', help='column denoting aggregate note count')
parser.add_argument("--aggtext", default='note_text', help='column denoting aggregated note text in output')
parser.add_argument("--sasdata", help='SAS data file for input (in sas7bdat format)')
parser.add_argument("--outfile", help='output file for generated aggregate text (in csv format)')
args = parser.parse_args()
group_by = args.groupby
aggnotecount = args.aggnotecount
aggtext = args.aggtext
sasdata = args.sasdata
outfile = args.outfile


def deline_dataframe(df, group_by, agg_note_count, agg_text):
    delined = df.groupby(group_by).agg({
        agg_note_count: 'max',
        agg_text: '\n'.join
    }).reset_index()
    return delined


def build_final_dataset(delined, df_from_clarity):
    """Add back metadata"""
    df_final = pd.merge(
        delined,
        df_from_clarity.drop_duplicates(),
        how='left',
        on = [group_by, aggnotecount]
        # on=['note_id', 'note_line']
    )
    return df_final


def main():
    df = pd.read_sas(sasdata, encoding='cp1252')  # read in dataset with multiple lines
    delined = deline_dataframe(df, group_by, aggnotecount, aggtext)  # merge lines together
    del df[aggtext]
    df_final = build_final_dataset(delined, df)
    df_final.to_csv(outfile, index=False)  # output


if __name__ == "__main__":
    main()
