import logging
import re

def safe_division(numerator, denominator):
    if denominator == 0:
         raise ValueError("Cannot divide by zero")
    elif not(isinstance(numerator, float) or isinstance(denominator,int) or isinstance(numerator, int) or isinstance(denominator,float)):
         raise TypeError("wrong type input")
    else:
        return numerator / denominator



def parse_amount(amount):
    letter_symbols = r'[a-zA-Z]'
    all_letters = re.findall(letter_symbols , amount)
    all_letter_join = ",".join(all_letters)

    money_symbols = r'^[$€£]|[$€£]$|[,]|[$€£]|[!@#%^&*]'
    remove_money_symbols = re.sub(money_symbols,'',amount)

    dot = r'[.]'
    all_dots = re.findall(dot,amount)
    if  len(all_dots) == 0:
        return int(remove_money_symbols)
    elif len(all_dots) == 1:
        return float(remove_money_symbols)
    elif len(all_letters)>=1:
        raise ValueError(f"following letters found :{all_letter_join}")
    elif len(all_dots) >1:
        raise ValueError(f"more than 1 dot found:{len(all_dots)}")
print(parse_amount("$2,444.50"))
