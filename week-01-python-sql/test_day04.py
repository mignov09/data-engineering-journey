import pytest
from day04_error_handling import safe_division,parse_amount

def test_safe_divide_ok():
    assert safe_division(9,3)==3

def test_safe_divide_zero():
    with pytest.raises(ValueError):
       safe_division(10,0)

def test_safe_divide_wrong_type():
    with pytest.raises(TypeError):
        safe_division(10, "10")

def test_parse_amount_ok():
    assert parse_amount("$12.32")==12.32

def test_parse_amount_invalid():
    with pytest.raises(ValueError):
       parse_amount("abc")