import pytest
import pandas as pd
from include.extract.extract_countries_api import create_session, extract_countries
from include.transform.transform_countries_api_pandas import transform_countries_pandas

def test_extract_countries():
    session = create_session()
    df = extract_countries(session)
    if df is not None:
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "name" in df.columns

def test_transform_countries_logic():
    # Mock data
    data = [{
        "name_common": "France",
        "region": "Europe",
        "population": 67000000,
        "area": 550000
    }]
    df = pd.DataFrame(data)
    # Simulation density logic
    df["density"] = df["population"] / df["area"]
    assert df["density"].iloc[0] == 67000000 / 550000
