import pytest
import pandas as pd
from include.extract.extract_exchange_rates import extract_exchange_rates

def test_exchange_rates_response_structure():
    # Test simple pour vérifier que l'API est accessible et renvoie la structure attendue
    df = extract_exchange_rates()
    if df is not None:
        assert isinstance(df, pd.DataFrame)
        assert "base" in df.columns
        assert "date" in df.columns
        assert df["base"].iloc[0] == "GBP"
        # Vérifie la présence de quelques devises clés
        for col in ["USD", "EUR"]:
            assert col in df.columns
