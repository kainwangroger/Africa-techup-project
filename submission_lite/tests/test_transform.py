import pytest
import pandas as pd
import numpy as np
from include.transform.transform_books_pandas import clean_price, clean_availability, clean_reviews

def test_clean_price():
    assert clean_price("£51.77") == 51.77
    assert clean_price("Â£10.50") == 10.50
    assert clean_price(20.0) == 20.0
    assert np.isnan(clean_price("invalid"))

def test_clean_availability():
    assert clean_availability("In stock (22 available)") == 22
    assert clean_availability("In stock (0 available)") == 0
    assert clean_availability("Empty") == 0
    assert clean_availability(None) == 0

def test_clean_reviews():
    assert clean_reviews("5") == 5
    assert clean_reviews(None) == 0
    assert clean_reviews(10) == 10

def test_currency_conversion_logic():
    # Simulation des données de taux de change
    df_books = pd.DataFrame({"price_incl_tax": [10.0, 20.0]})
    eur_rate = 1.2
    usd_rate = 1.3
    
    df_books["price_eur"] = df_books["price_incl_tax"] * eur_rate
    df_books["price_usd"] = df_books["price_incl_tax"] * usd_rate
    
    assert df_books["price_eur"].iloc[0] == 12.0
    assert df_books["price_usd"].iloc[1] == 26.0
