
# Mental Health Navigator NLP Pipeline

## Table of Contents

* [About the Project](#about-the-project)
* [Running the Pipeline](#running-the-pipeline)
  * [Prerequisites](#prerequisites)
    * [Index Dataset](#index-dataset)
    * [Historical Dataset](#historical-dataset)
  * [Usage](#usage)
  * [Configuration Options](#configuration-options)
  * [Output](#output)
    * [NLP Positive](#nlp-positive)
    * [NLP Model](#nlp-model)
    * [NLP Index](#nlp-ndex)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)

## About the Project

This NLP pipeline will use the output of `bratdb` to extract the features used in the MH Navigator model.


## Running the Pipeline

### Prerequisites

* Assumes construction of regular expression file in `bratdb`'s tab-separated format.
* Python 3.8+
* Install `mhnav_pipeline` with `pip install .` 
* Two datasets for 'index' and 'historical' information.
* Copy the file `src/mhnav_pipeline/local/cleaning_template.py` to `src/mhnav_pipeline/local/cleaning.py`
  * This will be used for any local modifications required to remove boilerplate, etc.

#### Index Dataset

This dataset will be only those notes obtained for the 'index encounter'. In theory, these should be all the notes with the same encounter_id or associated with the same visit.

The format should be:
* `studyid`: Unique de-identified patient identifier. 
* `pat_enc_csn_id`: Encounter unique identifier.
* `note_date`: Date of the note.
* `note_text`: Full text of the note.
* `start_date`: A year before the patient encounter (encounter date - 365 days).
* `end_date`: The day before the patient encounter (encounter date - 1 day).

NB: I don't think `start_date` and `end_date` are actually needed in the calculations, but MUST be included at present due to implementation details. 

#### Historical Dataset

This dataset will be all the notes in the year prior of the 'index encounter', and should not include any notes from the 'index encounter'.

The format should be:
* `studyid`: Unique de-identified patient identifier. 
* `index_pat_enc_csn_id`: Unique identifier for the 'index encounter' with which this record is associated (i.e., this note is in the range -1 to -365 days before the encounter with `index_pat_enc_csn_id`). All of these should appear in the [Index Dataset](#Index-Dataset), though not all records in the [Index Dataset](#Index-Dataset) will necessarily for notes in the pre-period.
* `pat_enc_csn_id`: Unique identifier of the encounter associated with this note.
* `note_date`: Date of the note.
* `note_text`: Full text of the note.
* `start_date`: A year before the patient encounter (encounter date - 365 days).
* `end_date`: The day before the patient encounter (encounter date - 1 day).
 

### Usage

If you install `mhnav_pipeline`, you can run it directly from the command line, either with individual options:

    run-mhnav-pipeline --index-dataset DS1 --historical-dataset DS2 --regex-file RX

or by supplying a configuration file:

    run-mhnav-pipeline @pipeline.conf

where the configuration file would have the following format:

    --index-dataset 
    DS1 
    --historical-dataset
    DS2 
    --regex-file
    RX

The pipeline can also be run directly from Python (e.g., in a Jupyter Notebook).

```python
from mhnav_pipeline.main import build_datasets
import pathlib

positive, model, index = build_datasets(
    # required arguments, see [CONFIGURATION OPTIONS](#Configuration-Options) below
    index_dataset='CSV OR TABLENAME',
    historical_dataset='CSV OR TABLENAME',
    regex_file='brat_dump.regexify.compiled.clean.tsv',
    # optional
    outpath=pathlib.Path('/path/to/output/directory'),
    in_connection_string='mssql+pyodbc://SERVER/database?driver=SQL Server',
    out_connection_string=None,  # want to use data directly, not output it
    output_to_csv=False,  # don't write to CSV, I want to use data directly
    include_context=0,  # change to >0 to include a separate table/csv file that will contain context elements for NLP hits
)
```
    

#### Configuration Options

There are three primary requirements for the configuration.

1. `--index-dataset`. The index dataset should be a CSV file, a pandas dataframe (if calling directly), or a database table. For the database table, `--in-connection-string` must also be supplied. See section [Index Dataset](#Index-Dataset)
2. `--historical-dataset`. The historical dataset should be a CSV file, a pandas dataframe (if calling directly), or a database table. For the database table, `--in-connection-string` must also be supplied. See section [Historical Dataset](#Historical-Dataset)
3. `--regex-file`. The regex file supplied to [bratdb-apply method](https://github.com/kpwhri/bratdb#bdb-apply).

Optional arguments.

4. `--in-connection-string`. If the `--index-dataset` and/or `--historical-dataset` are database table names, then you need to tell the program how to find the table. [SQL Alchemy-style connection string](https://docs.sqlalchemy.org/en/14/core/connections.html). You may need to install additional packages (e.g., `pyodbc` if using `mssql+pyodbc`).
5. `--out-connection-string`. If you want to output the tables into a database, you need to specify which database. The tablenames will be auto-generated and be suffixed with the current datetime. [SQL Alchemy-style connection string](https://docs.sqlalchemy.org/en/14/core/connections.html). You may need to install additional packages (e.g., `pyodbc` if using `mssql+pyodbc`).
6. `--dont-output-to-csv`. By default, the application always outputs the result to CSV files. If you don't want to have those CSV files, include this option.
7. `--outpath`. Path to write output files. Defaults to current directory.
8. `--include-context`. If specifying an integer > 0, will output a separate table with regex debugging information. This can help to exclude boilerplate, etc.

### Output

Three tables are output by the pipeline. [nlp_positive](#NLP-Positive) and [nlp_model](#NLP-Model) are both used by the model, while [nlp_index](#NLP-Index) is mostly for debugging.

The following outlines the basic structure.

You can change the output directory by using the `--outpath` option or the `--out-connection-string` parameter.

#### NLP Positive

One record per positive index encounter:

```sql
CREATE TABLE [dbo].[_NLPPositive](
	[pat_enc_csn_id] [bigint] NOT NULL, --the pat_enc_csn_id for the index enc
	[note_count] [int] NOT NULL--number of distinct primary care dates in prior year with notes positive for ANY of the MH CUIs 
--(not just the CUIs needed for the model)
) 
```


#### NLP Model

One record per index enc/(prior) note date/concept term, containing only the historical CUIs needed for the model.

```sql
CREATE TABLE [dbo].[_NLPOutput](
	[pat_enc_csn_id] [bigint] NOT NULL, --the pat_enc_csn_id for the index enc – foreign key to _NLPPositive
	[note_date] [date] NOT NULL, --either for the index enc or those prior
	[concept_term] [nvarchar](50) NOT NULL --one record per concept or term per note_date – see page 2 of this document for details
)
```

#### NLP Index

A table mostly for debugging or understanding how the model might go wrong.

One record per index enc/(index) note date/concept_term, containing only the index CUIs (not needed as part of the model – would be for debuggin). Limit to CUIs that would be relevant to the model.

```sql

CREATE TABLE [dbo].[_NLPIndex](
	[pat_enc_csn_id] [bigint] NOT NULL, --the pat_enc_csn_id for the index enc – foreign key to _NLPPositive
	[note_date] [date] NOT NULL, --not strictly needed, since enc date is recorded elsewhere – fine to drop if preferred
	[concept_term] [nvarchar](50) NOT NULL, --one record per concept or term per note_date – see page 2 of this document for details
	[text_string] [nvarchar](250) NULL  -- the text string from which the concept_term was derived
)
```

<!-- CONTRIBUTING -->
## Contributing

Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request


<!-- LICENSE -->
## License

Distributed under the MIT License. 

See `LICENSE` or https://kpwhri.mit-license.org for more information.



<!-- CONTACT -->
## Contact

Please use the [issue tracker](https://github.com/kpwhri/mhnav_pipeline/issues). 


<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/kpwhri/mhnav_pipeline.svg?style=flat-square
[contributors-url]: https://github.com/kpwhri/mhnav_pipeline/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/kpwhri/mhnav_pipeline.svg?style=flat-square
[forks-url]: https://github.com/kpwhri/mhnav_pipeline/network/members
[stars-shield]: https://img.shields.io/github/stars/kpwhri/mhnav_pipeline.svg?style=flat-square
[stars-url]: https://github.com/kpwhri/mhnav_pipeline/stargazers
[issues-shield]: https://img.shields.io/github/issues/kpwhri/mhnav_pipeline.svg?style=flat-square
[issues-url]: https://github.com/kpwhri/mhnav_pipeline/issues
[license-shield]: https://img.shields.io/github/license/kpwhri/mhnav_pipeline.svg?style=flat-square
[license-url]: https://kpwhri.mit-license.org/
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/company/kaiser-permanente-washington
<!-- [product-screenshot]: images/screenshot.png -->