from pydantic import BaseModel, Field
from enum import Enum

class GroqModelName(Enum):
    llama="llama-3.1-8b-instant" #Cheapest model
    qwen="qwen/qwen3-32b" #On the more expensive side but good at job


class GroqModelParams(BaseModel):
    """
    Parameters for the LLM demo flow.
    """

    model_name: GroqModelName = Field(
        default=GroqModelName.llama, 
        description="Model identifier to use via Groq.",
    )

#For calculating run costs
groq_costs = {
	GroqModelName.llama: {'i':0.05,'o':0.08},
	GroqModelName.qwen: {'i':0.29, 'o':0.59}
}
