import pytest

from lastlib_swarm.pricing import LEGACY_MODEL, estimate_cost, format_usd, model_price


def test_prices_uncached_cached_and_output_tokens_separately() -> None:
    cost = estimate_cost(
        model="gpt-5.6-luna",
        input_tokens=1_000_000,
        cached_input_tokens=200_000,
        output_tokens=100_000,
    )

    assert cost.estimated_usd == pytest.approx(0.284)
    assert cost.priced_tokens == 1_100_000
    assert cost.unpriced_tokens == 0
    assert format_usd(cost) == "$0.28"


def test_supports_configurable_openai_models_and_snapshots() -> None:
    assert LEGACY_MODEL == "gpt-5.6-luna"
    assert model_price("gpt-5.6-sol") is not None
    assert model_price("gpt-5.6-terra") is not None
    assert model_price("gpt-5.6-luna") is not None
    assert model_price("gpt-5.5") is not None
    assert model_price("gpt-5.4") is not None
    assert model_price("gpt-5.5-2026-04-23") == model_price("gpt-5.5")


def test_unknown_model_preserves_unpriced_token_count() -> None:
    cost = estimate_cost(
        model="custom-model",
        input_tokens=100,
        cached_input_tokens=50,
        output_tokens=20,
    )

    assert cost.estimated_usd == 0
    assert cost.unpriced_tokens == 120
    assert cost.unknown_models == ("custom-model",)
