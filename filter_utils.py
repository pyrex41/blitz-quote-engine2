from pydantic import BaseModel
from typing import List, Optional
import logging
class Quote(BaseModel):
    age: int
    gender: str
    plan: str
    tobacco: int
    rate: float
    discount_rate: float
    discount_category: Optional[str] = None


class QuoteInt(BaseModel):
    age: int
    gender: str
    plan: str
    tobacco: int
    rate: int
    discount_rate: int
    discount_category: Optional[str] = None
class QuoteResponse(BaseModel):
    naic: str
    group: int
    company_name: str
    quotes: List[Quote | QuoteInt]

class QuoteComparison(BaseModel):
    has_differences: bool
    db_quotes: List[QuoteResponse]
    csg_quotes: List[QuoteResponse]
    differences: Optional[List[str]] = None

def filter_quote_fields(quoteResponse):
    desired_fields = {
        'age', 'age_increases', 'company_base', 'discounts', 'discount_category', 'fees', 'gender',
        'plan', 'rate', 'rate_increases', 'rating_class', 'tobacco', 'view_type',
        'location_base'
    }
    filtered_quotes = []
    quotes, label = quoteResponse
    for quote in quotes:
        rating_class = quote.get('rating_class')
        naic = quote['company_base'].get('naic')
        if quote.get('select'):
            continue
        if quote.get('rating_class') not in [None, '', 'Standard', 'Achieve', 'Value']:
            if naic == '79413':
                if 'Standard' in rating_class and 'Household' not in rating_class:
                    pass    
                else:
                    continue
            else:
                continue

        d = {field: quote[field] for field in desired_fields}
        if len(d['location_base']['zip5']) > 0:
            d['location'] = d['location_base']['zip5']
        else:
            d['location'] = d['location_base']['county']
        comp = d.pop('company_base')
        d.pop('location_base')
        d['naic'] = comp.get('naic')
        d['name'] = comp.get('name')
        rate = d.pop('rate')
        d['rate'] = rate.get('month', 0) / 100
        # debug
        if d['rate'] == 214.88:
            with open('debug.log', 'a') as f:
                f.write(f"Found rate match: {d}\n")
        if d['naic'] == '60380' and d['age'] == 65 and d['plan'] == 'G' and d['gender'] == 'M' and d['tobacco'] == 0:
            with open('debug.log', 'a') as f:
                f.write(f"Found AFLAC rate: {d}\n")
        filtered_quotes.append(d)

    # Check for duplicate quotes and keep only the one with the higher rate
    unique_quotes = {}
    for quote in filtered_quotes:
        key = (quote['naic'], quote['tobacco'], quote['age'], quote['plan'], quote['gender'])
        if key in unique_quotes:
            if quote['rate'] > unique_quotes[key]['rate']:
                unique_quotes[key] = quote
        else:
            unique_quotes[key] = quote
    
    filtered_quotes = list(unique_quotes.values())
    return filtered_quotes


def use_int(quote):
    return QuoteInt(
        age=quote.age,
        gender=quote.gender,
        plan=quote.plan,
        tobacco=quote.tobacco,
        rate=int(quote.rate*100),
        discount_rate=int(quote.discount_rate*100),
        discount_category=quote.discount_category
    )

def filter_quote(quote):
    desired_fields = {
        'age', 'age_increases', 'company_base', 'discounts', 'discount_category', 'fees', 'gender',
        'plan', 'rate', 'rate_increases', 'rating_class', 'tobacco', 'view_type',
        'location_base'
    }

    rating_class = quote.get('rating_class')
    naic = quote['company_base'].get('naic')
    if quote.get('select'):
        return None
    if quote.get('rating_class') not in [None, '', 'Standard', 'Achieve', 'Value']:
        if naic == '79413':
            if 'Standard' in rating_class and 'Household' not in rating_class:
                pass    
            else:
                return None
        else:
            return None

    d = {field: quote[field] for field in desired_fields}
    if len(d['location_base']['zip5']) > 0:
        d['location'] = d['location_base']['zip5']
    else:
        d['location'] = d['location_base']['county']
    comp = d.pop('company_base')
    d.pop('location_base')
    d['naic'] = comp.get('naic')
    d['name'] = comp.get('name')
    rate = d.pop('rate')
    d['rate'] = rate.get('month', 0) / 100
    return d


def use_int(quote):
    return QuoteInt(
        age=quote.age,
        gender=quote.gender,
        plan=quote.plan,
        tobacco=quote.tobacco,
        rate=int(quote.rate*100),
        discount_rate=int(quote.discount_rate*100),
        discount_category=quote.discount_category
    )
