"""Tests that verify the rate limiter skips sleeping for cache-hit responses.

The key invariant: time.sleep must NOT be called for responses where
resp.from_cache is True.  Only real network responses should count against
the rate-limit budget.

Design: _wait_rate_limit() is called AFTER a confirmed network response
(post-response gate).  Cache-hit responses bypass _wait_rate_limit()
entirely so they never sleep and never update the rate-limit timestamp.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from legalize.fetcher.base import HttpClient


class ConcreteHttpClient(HttpClient):
    """Minimal concrete subclass used only in this test module."""

    def get_text(self, norm_id: str) -> bytes:
        return self._get(f"{self._base_url}/text/{norm_id}")

    def get_metadata(self, norm_id: str) -> bytes:
        return self._get(f"{self._base_url}/meta/{norm_id}")


def _make_response(*, from_cache: bool, status: int = 200, content: bytes = b"data"):
    """Build a mock requests.Response-like object."""
    resp = MagicMock()
    resp.status_code = status
    resp.content = content
    resp.raise_for_status = MagicMock()
    resp.from_cache = from_cache
    return resp


class TestCacheHitSkipsRateLimit:
    def test_sleep_not_called_for_cache_hit(self):
        """time.sleep must not be called when resp.from_cache is True.

        Sequence: network call (sets _last_request), then cache hit.
        The cache hit must return without any sleep.
        """
        # 0.5 rps → 2-second minimum interval; any sleep would be very obvious.
        client = ConcreteHttpClient(requests_per_second=0.5)

        network_resp = _make_response(from_cache=False)
        cache_resp = _make_response(from_cache=True)
        client._session.request = MagicMock(side_effect=[network_resp, cache_resp])

        with patch("legalize.fetcher.base.time.sleep") as mock_sleep:
            client._get("https://example.com/law/1")  # network hit (no prior call → no sleep)
            client._get("https://example.com/law/1")  # cache hit → no sleep

        # Neither call should have caused a sleep:
        # - First (network): _last_request was 0.0 → elapsed is huge → no sleep
        # - Second (cache): bypasses _wait_rate_limit entirely → no sleep
        mock_sleep.assert_not_called()
        client.close()

    def test_sleep_called_after_rapid_second_network_request(self):
        """After two rapid real network calls the rate limiter must sleep.

        With the post-response gate: the sleep happens after the first
        network response if the previous call was too recent.  Here we
        seed _last_request to a non-zero value so the first call is also
        subject to the rate limit (simulates a call that just happened).
        """
        client = ConcreteHttpClient(requests_per_second=10.0)  # 100 ms interval
        # Seed _last_request so the first call looks like it just happened.
        client._last_request = 1000.0

        network_resp1 = _make_response(from_cache=False)
        network_resp2 = _make_response(from_cache=False)
        client._session.request = MagicMock(side_effect=[network_resp1, network_resp2])

        # Monotonic sequence (two calls to _wait_rate_limit, each reads twice):
        # Call 1 → _wait_rate_limit:
        #   elapsed-read=1000.05, elapsed=1000.05-1000.0=0.05 < 0.1 → sleep(0.05)
        #   record=1000.05
        # Call 2 → _wait_rate_limit:
        #   elapsed-read=1000.08, elapsed=1000.08-1000.05=0.03 < 0.1 → sleep(0.07)
        #   record=1000.08
        with patch("legalize.fetcher.base.time.sleep") as mock_sleep:
            with patch(
                "legalize.fetcher.base.time.monotonic",
                side_effect=[1000.05, 1000.05, 1000.08, 1000.08],
            ):
                client._get("https://example.com/law/1")
                client._get("https://example.com/law/2")

        assert mock_sleep.call_count == 2
        client.close()

    def test_cache_hit_does_not_reset_rate_limit_timer(self):
        """A cache hit between two network calls must not grant the second
        network call a free pass on the rate-limit wait.

        Sequence: net1 (sets _last_request=T), cache (no update), net2.
        net2 must see elapsed from T, not from a reset 0.
        """
        client = ConcreteHttpClient(requests_per_second=10.0)  # 100 ms interval

        net1 = _make_response(from_cache=False)
        cached = _make_response(from_cache=True)
        net2 = _make_response(from_cache=False)
        client._session.request = MagicMock(side_effect=[net1, cached, net2])

        sleep_calls = []

        def record_sleep(secs):
            sleep_calls.append(secs)

        # Seed _last_request so net1 also falls within the rate-limit window.
        client._last_request = 1000.0

        with patch("legalize.fetcher.base.time.sleep", side_effect=record_sleep):
            with patch(
                "legalize.fetcher.base.time.monotonic",
                # net1 _wait_rate_limit: elapsed-read=1000.05, record=1000.05
                #   elapsed=0.05 < 0.1 → sleep(0.05)
                # cached: no _wait_rate_limit call at all
                # net2 _wait_rate_limit: elapsed-read=1000.08, record=1000.08
                #   elapsed=1000.08-1000.05=0.03 < 0.1 → sleep(0.07)
                side_effect=[1000.05, 1000.05, 1000.08, 1000.08],
            ):
                client._get("https://example.com/law/1")  # net1
                client._get("https://example.com/law/1")  # cached (no sleep, no timestamp update)
                client._get("https://example.com/law/2")  # net2 (sees elapsed from net1)

        # net1: elapsed since seed is 0.05 s < 0.1 s → one sleep
        # cache hit: no _wait_rate_limit call → no sleep, no timestamp update
        # net2: elapsed since net1 is 0.03 s < 0.1 s → one more sleep
        assert len(sleep_calls) == 2
        assert sleep_calls[0] == pytest.approx(0.05)
        assert sleep_calls[1] == pytest.approx(0.07)
        client.close()

    def test_plain_session_no_from_cache_attr_treated_as_network(self):
        """A response from a plain requests.Session (no from_cache attr) must
        be treated as a network hit — _last_request must be updated."""
        client = ConcreteHttpClient(requests_per_second=10.0)  # 100 ms

        class PlainResponse:
            status_code = 200
            content = b"data"

            def raise_for_status(self):
                pass

        client._session.request = MagicMock(
            side_effect=[PlainResponse(), PlainResponse()]
        )

        assert client._last_request == 0.0

        with patch("legalize.fetcher.base.time.sleep"):
            with patch(
                "legalize.fetcher.base.time.monotonic",
                side_effect=[0.0, 0.0, 0.05, 0.05],
            ):
                client._get("https://example.com/a")
                client._get("https://example.com/b")

        # _last_request was updated after second plain response (= 0.05 from monotonic)
        assert client._last_request == 0.05
        client.close()

    def test_wall_clock_cache_hit_returns_fast(self):
        """Integration-style: with a large rate-limit interval, a network call
        followed by a cache-hit call must complete fast (< 0.5 s total)
        because the cache hit bypasses all sleeping."""
        # 0.5 rps → 2 s interval. Without the fix the test would take ~2 s.
        client = ConcreteHttpClient(requests_per_second=0.5)

        network_resp = _make_response(from_cache=False)
        cache_resp = _make_response(from_cache=True)
        client._session.request = MagicMock(side_effect=[network_resp, cache_resp])

        start = time.monotonic()
        client._get("https://example.com/law/1")  # network (no prior → no sleep)
        client._get("https://example.com/law/1")  # cache hit → no sleep
        elapsed = time.monotonic() - start

        # Should finish almost instantly — definitely under 0.5 s.
        assert elapsed < 0.5, f"Cache hit took {elapsed:.3f}s — rate limit was applied"
        client.close()

    def test_multiple_consecutive_cache_hits_all_fast(self):
        """Multiple consecutive cache hits after one network call must all
        return without sleeping."""
        client = ConcreteHttpClient(requests_per_second=0.5)  # 2 s interval

        responses = [_make_response(from_cache=False)] + [
            _make_response(from_cache=True) for _ in range(5)
        ]
        client._session.request = MagicMock(side_effect=responses)

        with patch("legalize.fetcher.base.time.sleep") as mock_sleep:
            for _ in range(6):
                client._get("https://example.com/law/1")

        # Only the network call contributes to rate limiting.
        # The first network call: _last_request was 0.0 → no sleep.
        # All 5 cache hits: bypassed entirely → no sleep.
        mock_sleep.assert_not_called()
        client.close()
