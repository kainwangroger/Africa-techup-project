import pytest
import pandas as pd
from include.extract.scrap_links import create_session

def test_session_creation():
    session = create_session()
    assert session is not None
    assert "User-Agent" in session.headers

def test_extract_books_structure():
    # Mock or use a small sample if possible
    pass
