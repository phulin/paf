from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

TOKENS_PER_MILLION = 1_000_000
LEGACY_MODEL = "gpt-5.6-luna"


@dataclass(frozen=True)
class ModelPrice:
    input_per_million: float
    cached_input_per_million: float
    output_per_million: float


# Standard API text-token prices published at https://developers.openai.com/api/docs/models on
# 2026-08-10. These estimates
# intentionally exclude long-context multipliers, cache-write premiums, regional processing,
# and tool-call charges.
MODEL_PRICES: dict[str, ModelPrice] = {
    "gpt-5.6": ModelPrice(5.0, 0.5, 30.0),
    "gpt-5.6-sol": ModelPrice(5.0, 0.5, 30.0),
    "gpt-5.6-terra": ModelPrice(2.0, 0.2, 12.0),
    "gpt-5.6-luna": ModelPrice(0.2, 0.02, 1.2),
    "gpt-5.5": ModelPrice(5.0, 0.5, 30.0),
    "gpt-5.4": ModelPrice(2.5, 0.25, 15.0),
}


@dataclass(frozen=True)
class CostEstimate:
    estimated_usd: float = 0.0
    priced_tokens: int = 0
    unpriced_tokens: int = 0
    inferred_runs: int = 0
    unknown_models: tuple[str, ...] = ()

    @property
    def measured(self) -> bool:
        return self.priced_tokens > 0 or self.unpriced_tokens > 0

    def __add__(self, other: CostEstimate) -> CostEstimate:
        return CostEstimate(
            estimated_usd=self.estimated_usd + other.estimated_usd,
            priced_tokens=self.priced_tokens + other.priced_tokens,
            unpriced_tokens=self.unpriced_tokens + other.unpriced_tokens,
            inferred_runs=self.inferred_runs + other.inferred_runs,
            unknown_models=tuple(sorted(set(self.unknown_models) | set(other.unknown_models))),
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self) | {"measured": self.measured}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> CostEstimate:
        unknown = value.get("unknown_models", ())
        return cls(
            estimated_usd=float(value.get("estimated_usd", 0)),
            priced_tokens=int(value.get("priced_tokens", 0)),
            unpriced_tokens=int(value.get("unpriced_tokens", 0)),
            inferred_runs=int(value.get("inferred_runs", 0)),
            unknown_models=(
                tuple(str(item) for item in unknown) if isinstance(unknown, (list, tuple)) else ()
            ),
        )


def model_price(model: str) -> ModelPrice | None:
    if model in MODEL_PRICES:
        return MODEL_PRICES[model]
    for name, price in MODEL_PRICES.items():
        if model.startswith(f"{name}-20"):
            return price
    return None


def estimate_cost(
    *,
    model: str,
    input_tokens: int,
    cached_input_tokens: int,
    output_tokens: int,
    inferred: bool = False,
) -> CostEstimate:
    total_tokens = input_tokens + output_tokens
    if total_tokens == 0:
        return CostEstimate()
    price = model_price(model)
    if price is None:
        return CostEstimate(
            unpriced_tokens=total_tokens,
            inferred_runs=int(inferred),
            unknown_models=(model,),
        )
    cached = min(max(cached_input_tokens, 0), max(input_tokens, 0))
    uncached = max(input_tokens - cached, 0)
    usd = (
        uncached * price.input_per_million
        + cached * price.cached_input_per_million
        + output_tokens * price.output_per_million
    ) / TOKENS_PER_MILLION
    return CostEstimate(
        estimated_usd=usd,
        priced_tokens=total_tokens,
        inferred_runs=int(inferred),
    )


def format_usd(cost: CostEstimate) -> str:
    if not cost.measured:
        return "awaiting measured usage"
    if cost.priced_tokens == 0:
        return "unavailable for unpriced model"
    amount = cost.estimated_usd
    rendered = f"${amount:,.4f}" if amount < 0.01 else f"${amount:,.2f}"
    if cost.unpriced_tokens:
        rendered += f" + {cost.unpriced_tokens:,} unpriced tokens"
    return rendered
