from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from mainsequence.client.base import BaseObjectOrm, BasePydanticModel

COMPOSITE_TO_ISO = {
    "AR": "XBUE",
    "AU": "XASX",
    "BZ": "BVMF",
    "CN": "XTSE",
    "CB": "XBOG",
    "CH": "XSHG",
    "CI": "XSGO",
    "CP": "XPRA",
    "DC": "XCSE",
    "FH": "XHEL",
    "FP": "XPAR",
    "GA": "ASEX",
    "GR": "XFRA",
    "HK": "XHKG",
    "IE": "XDUB",
    "IM": "XMIL",
    "IN": "XBOM",
    "IT": "XTAE",
    "JP": "XTKS",
    "KS": "XKRX",
    "KZ": "AIXK",
    "LN": "XLON",
    "MM": "XMEX",
    "MK": "XKLS",
    "NA": "XAMS",
    "PL": "XLIS",
    "PM": "XPHS",
    "PW": "XWAR",
    "RO": "XBSE",
    "SA": "XSAU",
    "SM": "XMAD",
    "SS": "XSTO",
    "SW": "XSWX",
    "TH": "XBKK",
    "TI": "XIST",
    "TT": "XTAI",
    "US": "XNYS",
    "AT": "XWBO",
    "BB": "XBRU",
}


class Calendar(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    name: str
    calendar_dates: dict | None = None

    def __str__(self):
        return self.name

    def __repr__(self) -> str:
        return self.name


def _set_query_param_on_url(url: str, key: str, value) -> str:
    """
    Add or replace a query parameter in a URL without disturbing others (e.g., offset/page).
    Works with absolute or relative URLs.
    """
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = str(value)
    new_query = urlencode(q, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
