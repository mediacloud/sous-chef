from pydantic import BaseModel, Field
from typing import ClassVar
from enum import Enum

class GroqModelName(str, Enum):
    gpt_oss_20b = "groq/openai/gpt-oss-120b",  # ⭐️
    gpt_oss_120b="groq/openai/gpt-oss-120b",  # ⭐️
    qwen_36_27b="groq/qwen/qwen3.6-27b"  # recall terrible!


class GroqModelParams(BaseModel):
    """
    Parameters for the LLM demo flow.
    """

    _component_hint: ClassVar[str] = "LLMModelParams"

    model_name: GroqModelName = Field(
        default=GroqModelName.gpt_oss_20b,
        title="LLM Model Identifier",
        description="Model identifier to use via Groq.",
    )

#For calculating run costs
groq_costs = {
    GroqModelName.gpt_oss_20b: {'i':0.075, 'o':0.3},
    GroqModelName.qwen_36_27b: {'i':0.6, 'o':3},
    GroqModelName.gpt_oss_120b: {'i':0.15, 'o':0.60}
}
