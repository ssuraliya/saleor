import json
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from typing import List

from prices import Money, TaxedMoney
from pydantic import BaseModel


class TaxError(Exception):
    """Default tax error."""


def zero_money(currency: str) -> Money:
    """Return a money object set to zero.

    This is a function used as a model's default.
    """
    return Money(0, currency)


def zero_taxed_money(currency: str) -> TaxedMoney:
    zero = zero_money(currency)
    return TaxedMoney(net=zero, gross=zero)


@dataclass(frozen=True)
class TaxType:
    """Dataclass for unifying tax type object that comes from tax gateway."""

    code: str
    description: str


class WebhookResponseBase(BaseModel):
    class Config:
        allow_mutation = False
        json_loads = partial(json.loads, parse_float=Decimal)


class TaxLineData(WebhookResponseBase):
    tax_rate: Decimal
    total_gross_amount: Decimal
    total_net_amount: Decimal


class TaxData(WebhookResponseBase):
    shipping_price_gross_amount: Decimal
    shipping_price_net_amount: Decimal
    shipping_tax_rate: Decimal
    lines: List[TaxLineData]
