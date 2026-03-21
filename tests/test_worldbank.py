import pytest
import pandas as pd
from include.extract.extract_worldbank_api import create_session, extract_population_all

def test_extract_worldbank():
    session = create_session()
    df = extract_population_all(session)
    if df is not None:
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "country_name" in df.columns
        assert "year" in df.columns
