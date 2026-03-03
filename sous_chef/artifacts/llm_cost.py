"""
LLM-related artifacts.
"""
from typing import Iterable, ClassVar, Any
from .base import BaseArtifact


class LLMCostSummary(BaseArtifact):
	artifact_type: ClassVar[str] = "LLM_usage_summary"

	total_requests: int
	total_prompt_tokens: int
	total_completion_tokens: int
	total_tokens: int

	@classmethod
	def from_groq_summaries(cls, usages: Iterable[Any]) -> 'LLMCostSummary':
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

		return cls(
			total_requests = total_requests,
			total_prompt_tokens = total_prompt_tokens,
			total_completion_tokens = total_completion_tokens,
			total_tokens = total_tokens,
		)

	def _summary(self) -> str:
		"""Generate a human-readable summary."""
		return (
			f"{self.total_tokens} tokens "
			f"({self.total_prompt_tokens} prompt, "
			f"{self.total_completion_tokens} completion) "
			f"across {self.total_requests} requests"
		)