import numpy as np
import pandas as pd
import pytest

from mhnav_pipeline.local.cleaning_template import clean_text


@pytest.mark.parametrize('null_value', [
    None, np.nan, pd.NA,
])
def test_cleaning_is_null(null_value):
    clean_text(null_value)
    assert True
