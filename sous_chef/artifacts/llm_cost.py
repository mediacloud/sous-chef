"""
LLM-related artifacts.
"""
from typing import Iterable, ClassVar, Any, Optional
from .base import BaseArtifact
from ..params import groq_costs, GroqModelName


class LLMCostSummary(BaseArtifact):
	artifact_type: ClassVar[str] = "llm_usage_summary"

	provider: str = 'groq'
	model: GroqModelName

	total_requests: int
	total_prompt_tokens: int
	total_completion_tokens: int
	total_tokens: int

	# Cached pricing metadata (USD per 1M tokens)
	price_per_million_input_usd: Optional[float] = None
	price_per_million_output_usd: Optional[float] = None

	# Computed total costs (USD)
	total_input_cost_usd: Optional[float] = None
	total_output_cost_usd: Optional[float] = None
	total_cost_usd: Optional[float] = None

	@classmethod
	def from_groq_summaries(cls, model: GroqModelName, usages: Iterable[Any]) -> 'LLMCostSummary':
		"""
		Aggregate Groq CompletionUsage objects into simple totals.

		Args:
			usages: Iterable of objects with .prompt_tokens, .completion_tokens,
					and .total_tokens attributes (e.g., Groq CompletionUsage).

		Returns:
			Dict with total_requests and summed token counts.
		"""
		total_requests = 0
		total_prompt_tokens = 0
		total_completion_tokens = 0
		total_tokens = 0

		for u in usages:
			if u is None:
				continue
			total_requests += 1

			# getattr(..., 0) + "or 0" handles None values gracefully
			total_prompt_tokens += (getattr(u, "prompt_tokens", 0) or 0)
			total_completion_tokens += (getattr(u, "completion_tokens", 0) or 0)
			total_tokens += (getattr(u, "total_tokens", 0) or 0)

	   # Look up pricing for this model, if available
		prices = groq_costs.get(model)
		price_i = prices["i"] if prices else None  # USD per 1M input tokens
		price_o = prices["o"] if prices else None  # USD per 1M output tokens

		total_input_cost = None
		total_output_cost = None
		total_cost = None

		if price_i is not None:
			total_input_cost = (total_prompt_tokens / 1_000_000.0) * price_i
		if price_o is not None:
			total_output_cost = (total_completion_tokens / 1_000_000.0) * price_o
		if total_input_cost is not None or total_output_cost is not None:
			total_cost = (total_input_cost or 0.0) + (total_output_cost or 0.0)

		return cls(
			provider="groq",
			model=model,
			total_requests=total_requests,
			total_prompt_tokens=total_prompt_tokens,
			total_completion_tokens=total_completion_tokens,
			total_tokens=total_tokens,
			price_per_million_input_usd=price_i,
			price_per_million_output_usd=price_o,
			total_input_cost_usd=total_input_cost,
			total_output_cost_usd=total_output_cost,
			total_cost_usd=total_cost,
		)

	def _summary(self) -> str:
		"""
		Human-readable summary, including estimated cost if available.
		"""
		base = (
			f"{self.total_tokens} tokens "
			f"({self.total_prompt_tokens} prompt, "
			f"{self.total_completion_tokens} completion) "
			f"across {self.total_requests} requests "
			f"on {self.provider}:{self.model}"
		)
		if self.total_cost_usd is not None:
			return f"{base} (estimated cost: ${self.total_cost_usd:.4f} USD)"
		return base
	
	def get_artifact_description(self) -> str:
		"""Generate a description for Prefect artifact display."""
		cost_info = ""
		if self.total_cost_usd is not None:
			cost_info = f" - ${self.total_cost_usd:.4f} USD"
		return f"LLM Cost Summary: {self.provider}:{self.model} ({self.total_tokens:,} tokens{cost_info})"