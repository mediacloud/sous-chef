from pydantic import BaseModel, Field
from typing import ClassVar
from enum import Enum

class GroqModelName(str, Enum):
    llama="llama-3.1-8b-instant" #Cheapest model
    qwen="qwen/qwen3-32b" #On the more expensive side but good at job
    llama_versatile="llama-3.3-70b-versatile" # expensive but validated for quote extraction


class GroqModelParams(BaseModel):
    """
    Parameters for the LLM demo flow.
    """

    _component_hint: ClassVar[str] = "LLMModelParams"

    model_name: GroqModelName = Field(
        default=GroqModelName.llama, 
        title="LLM Model Identifier",
        description="Model identifier to use via Groq.",
    )

#For calculating run costs
groq_costs = {
	GroqModelName.llama: {'i':0.05,'o':0.08},
	GroqModelName.qwen: {'i':0.29, 'o':0.59}
}
