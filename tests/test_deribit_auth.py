"""DeribitCredentials + auth behaviour (offline unit tests).

Live auth is exercised separately; here we cover:
- repr redaction so creds never leak into logs / task output
- interval resolution (auto/raw/100ms/agg2 + auth status)
- validation of the ``add_deribit_trades`` interval argument
"""
from __future__ import annotations

from chronos import Recorder, DeribitCredentials, Gateway


def _gw(tmp_path) -> Gateway:
    return Gateway(Recorder(tmp_path))


def test_credentials_repr_hides_secret_and_most_of_id():
    c = DeribitCredentials(client_id="fNKohhkR", client_secret="super-secret-value")
    r = repr(c)
    assert "super-secret-value" not in r
    assert "fNKohhkR" not in r, "full client_id should not appear"
    assert "fN" in r and "***" in r


def test_credentials_repr_handles_empty_id():
    r = repr(DeribitCredentials("", "secret"))
    assert "secret" not in r
    assert "***" in r


def test_str_never_leaks_secret():
    c = DeribitCredentials(client_id="abc", client_secret="SECRET")
    assert "SECRET" not in f"{c!r}"
    assert "SECRET" not in f"{c}"


def test_interval_auto_with_creds_resolves_to_raw(tmp_path):
    g = _gw(tmp_path)
    g.add_deribit_trades(
        "BTC-PERPETUAL",
        credentials=DeribitCredentials("cid", "secret"),
        interval="auto",
    )
    assert g._deribit_resolve_interval(authed=True) == "raw"
    assert g._deribit_resolve_interval(authed=False) == "100ms"


def test_interval_auto_without_creds_resolves_to_100ms(tmp_path):
    g = _gw(tmp_path)
    g.add_deribit_trades("BTC-PERPETUAL", interval="auto")
    assert g._deribit_resolve_interval(authed=False) == "100ms"


def test_interval_raw_without_auth_falls_back(tmp_path):
    g = _gw(tmp_path)
    g.add_deribit_trades("BTC-PERPETUAL", interval="raw")
    assert g._deribit_resolve_interval(authed=False) == "100ms"


def test_explicit_interval_100ms_passes_through(tmp_path):
    g = _gw(tmp_path)
    g.add_deribit_trades("BTC-PERPETUAL", interval="100ms")
    assert g._deribit_resolve_interval(authed=False) == "100ms"
    assert g._deribit_resolve_interval(authed=True) == "100ms"


def test_explicit_interval_agg2_passes_through(tmp_path):
    g = _gw(tmp_path)
    g.add_deribit_trades("BTC-PERPETUAL", interval="agg2")
    assert g._deribit_resolve_interval(authed=True) == "agg2"


def test_invalid_interval_raises(tmp_path):
    import pytest
    g = _gw(tmp_path)
    with pytest.raises(ValueError):
        g.add_deribit_trades("BTC-PERPETUAL", interval="nope")


def test_first_credentials_win_for_multi_instrument(tmp_path):
    g = _gw(tmp_path)
    cred_a = DeribitCredentials("a", "A")
    cred_b = DeribitCredentials("b", "B")
    g.add_deribit_trades("BTC-PERPETUAL", credentials=cred_a)
    # Second call with different creds should be ignored (single session).
    g.add_deribit_trades("ETH-PERPETUAL", credentials=cred_b)
    assert g._deribit_credentials is cred_a
    assert g._deribit_instruments == ["BTC-PERPETUAL", "ETH-PERPETUAL"]
