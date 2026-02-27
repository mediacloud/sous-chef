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
from typing import Any, Dict, Generic, Optional, TypeVar

from pydantic import BaseModel, Field
from ..secrets import get_llm_api_key
import instructor
from litellm import completion


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
        return result


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
            output = self.client.execute(
                prompt=prompt,
                response_model=self.output_model,
                system_prompt=None,
                **client_kwargs,
            )
            return TaskOutcome[OutputModelT](
                status=OutcomeStatus.SUCCESS,
                output=output,
                metadata={},
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

