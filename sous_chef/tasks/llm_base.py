from __future__ import annotations

"""
Core abstractions for structured LLM tasks in sous-chef.

This module provides:
- OutcomeStatus / TaskOutcome: standardized container for LLM task results
- LLMModelClient: minimal abstract interface for LLM providers
- LiteLLMClient: default implementation using litellm + HF instruct-style models
- BaseLLMTask: generic Pydantic-based task wrapper for structured prompts
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Generic, Optional, TypeVar, Iterable, Callable, List

from pydantic import BaseModel, Field
from ..secrets import get_llm_api_key
from ..params import GroqModelName
import instructor
from litellm import completion
from groq import Groq


T = TypeVar("T", bound=BaseModel)


class OutcomeStatus(str, Enum):
    """High-level status for an LLM task execution."""

    SUCCESS = "success"
    FAILURE = "failure"


class TaskOutcome(Generic[T], BaseModel):
    """
    Container for the result of a structured LLM task.

    Separates infra-level success/failure from the semantic content of the
    output model. Semantic uncertainty (e.g., "I don't know") should be
    represented inside the OutputModel itself.
    """

    status: OutcomeStatus
    output: Optional[T] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    raw_response: Dict[str, Any] = Field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """Convenience flag: True when status == SUCCESS and output is present."""

        return self.status == OutcomeStatus.SUCCESS and self.output is not None


class LLMModelClient(ABC):
    """
    Minimal abstract interface for LLM providers using Instructor.
    """
    
    def __init__(self, model_name: str, **kwargs: Any) -> None:
        self.model_name = model_name
        self.config = kwargs
    
    @abstractmethod
    def execute(
        self,
        prompt: str,
        response_model: type[BaseModel],
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> BaseModel:
        """
        Execute a prompt and return a validated Pydantic model.
        """



class LiteLLMClient(LLMModelClient):
    """
    Default LLM client using litellm and HF instruct-style models.

    Configuration:
        - model_name: HF instruct model (or any litellm-supported name)
        - api_base: optional custom base URL (from env or argument)
        - api_key: API key for the provider, resolved via secrets or env
    """

    def __init__(
        self,
        model_name: str = "Qwen/Qwen3-4B-Instruct-2507",
        provider: str = "huggingface",
        **kwargs: Any,
    ) -> None:
        """
        Instructor-based client using litellm for HuggingFace models by default.
        """
        super().__init__(model_name, provider=provider, **kwargs)
        
        import os
        
        self.provider = provider
        # Resolve API key using sous-chef secrets, then expose to litellm
        token = get_llm_api_key(provider=self.provider)
        if token and not os.getenv("HUGGINGFACE_API_KEY"):
            os.environ["HUGGINGFACE_API_KEY"] = token
        
        # Normalize model name for litellm (huggingface/<model_name>)
        if model_name.startswith("huggingface/"):
            self.hf_model = model_name
        else:
            self.hf_model = f"huggingface/{model_name}"
        
        try:
            # Wrap litellm completion with Instructor in JSON_SCHEMA mode
            self.client = instructor.from_litellm(
                completion,
                mode=instructor.Mode.JSON,
                **kwargs,
            )
        except ImportError as exc:  # pragma: no cover - import error path
            raise ImportError(
                "instructor[litellm] is required for LiteLLMClient. "
                "Install with: pip install 'instructor[litellm]'"
            ) from exc
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize Instructor with litellm: {e}"
            ) from e
    
    def execute(
        self,
        prompt: str,
        response_model: type[BaseModel],
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> BaseModel:
        """
        Execute a prompt using Instructor + litellm and return a Pydantic model.
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        result = self.client.create(
            model=self.hf_model,
            messages=messages,
            response_model=response_model,
            max_retries=max_retries,
            **kwargs,
        )
        return result, None



class GroqClient(LLMModelClient):
    """
    LLM client using Groq + Instructor.

    Default model is a Groq-hosted Qwen variant; override model_name
    in flows/params as needed.
    """

    def __init__(
        self,
        model_name: GroqModelName = GroqModelName.llama,  # reasonable default
        provider: str = "groq",
        **kwargs: Any,
    ) -> None:
        super().__init__(model_name.value, provider=provider, **kwargs)

        import os

        self.provider = provider

        # Resolve API key via sous-chef secrets, then expose to Groq client
        api_key = get_llm_api_key(provider=self.provider)
        if api_key and not os.getenv("GROQ_API_KEY"):
            os.environ["GROQ_API_KEY"] = api_key

        # Initialize raw Groq client for authentication
        raw_client = Groq(api_key=api_key)

        try:
            # Wrap Groq client with Instructor using the provider string
            # "groq/<model_name>" follows the from_provider convention
            provider_str = f"groq/{self.model_name}"
            self.client = instructor.from_provider(provider_str)
            
        except ImportError as exc:
            raise ImportError(
                "instructor with Groq support is required for GroqClient. "
                "Install with: pip install 'instructor[groq]' groq"
            ) from exc
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize Instructor Groq client: {e}"
            ) from e

    def execute(
        self,
        prompt: str,
        response_model: type[BaseModel],
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> BaseModel:
        """
        Execute a prompt using Groq + Instructor and return a Pydantic model.
        """
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        #Register a callback for the duration of this execution to aggregate usage statistics
        usage_aggregator = []
        def groq_response_callback_hook(response):
            usage =  getattr(response, "usage", None)
            if usage:
                usage_aggregator.append(usage)

        self.client.on("completion:response", groq_response_callback_hook)

        result = self.client.create(
            messages=messages,
            response_model=response_model,
            max_retries=max_retries,
            **kwargs,
        )

        self.client.off("completion:response", groq_response_callback_hook)

        return result, usage_aggregator



InputModelT = TypeVar("InputModelT", bound=BaseModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)


class BaseLLMTask(Generic[InputModelT, OutputModelT]):
    """
    Generic base class for structured LLM tasks using Pydantic IO models.

    Subclasses should set:
        - input_model
        - output_model
        - prompt_template (string with {field} placeholders from InputModel)
    """

    input_model: type[InputModelT]
    output_model: type[OutputModelT]

    def __init__(
        self,
        client: LLMModelClient,
        task_name: str,
        description: str,
        prompt_template: str,
    ) -> None:
        self.client = client
        self.task_name = task_name
        self.description = description
        self.prompt_template = prompt_template

    def build_prompt(self, data: InputModelT) -> str:
        """
        Default prompt builder: simple .format() with the input model dict.
        """

        return self.prompt_template.format(**data.model_dump())

    def run(
        self,
        data: InputModelT,
        **client_kwargs: Any,
    ) -> TaskOutcome[OutputModelT]:
        """
        Execute the task for a single InputModel instance.
        """

        try:
            prompt = self.build_prompt(data)
            output, stats = self.client.execute(
                prompt=prompt,
                response_model=self.output_model,
                system_prompt=None,
                **client_kwargs,
            )

            metadata: Dict[str, Any] = {}
            if stats:
                metadata["usage_summaries"] = stats


            return TaskOutcome[OutputModelT](
                status=OutcomeStatus.SUCCESS,
                output=output,
                metadata=metadata,
                raw_response={},
            )
        except Exception as exc:
            return TaskOutcome[OutputModelT](
                status=OutcomeStatus.FAILURE,
                output=None,
                metadata={"error": str(exc), "task_name": self.task_name},
                raw_response={},
            )

    def run_many(
        self,
        items: list[InputModelT],
        **client_kwargs: Any,
    ) -> list[TaskOutcome[OutputModelT]]:
        """
        Execute the task for a list of InputModel instances.
        """

        return [self.run(item, **client_kwargs) for item in items]


def run_llm_task_over_rows(
    rows: Iterable[Any],
    llm_task: BaseLLMTask[Any, Any],
    build_input: Callable[[Any], BaseModel],
) -> tuple[List[TaskOutcome[Any]], List[Any]]:
    """
    Run a BaseLLMTask over an iterable of row-like objects.

    This is a core abstraction that is agnostic to the input container
    (DataFrame, list of dicts, etc). It returns:
      - list of TaskOutcome objects
      - list of provider usage objects (for cost aggregation), pulled from
        TaskOutcome.metadata under 'usage_summaries' or 'llm_usage'.
    """
    outcomes: List[TaskOutcome[Any]] = []
    usages: List[Any] = []

    for row in rows:
        outcome: TaskOutcome[Any] = llm_task.run(build_input(row))
        outcomes.append(outcome)

        if outcome.ok and outcome.metadata:
            usage = (
                outcome.metadata.get("usage_summaries")
                or outcome.metadata.get("llm_usage")
            )
            if usage is not None:
                # Allow either a single usage object or a list of them
                if isinstance(usage, list):
                    usages.extend(usage)
                else:
                    usages.append(usage)

    return outcomes, usages

