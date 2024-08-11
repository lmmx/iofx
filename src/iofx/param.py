from typing import Any
from collections.abc import Callable
from pydantic import BaseModel, Field
from inspect import Parameter, signature

__all__ = ("ParameterInfo", "extract_function_info")


class ParameterInfo(BaseModel):
    name: str
    type: Any
    default: Any = Field(default=Parameter.empty)


def extract_function_info(func: Callable) -> tuple[list[ParameterInfo], Any]:
    """Extract parameters and return type from function signature."""
    sig = signature(func)
    params = []
    for name, param in sig.parameters.items():
        param_type = param.annotation if param.annotation != Parameter.empty else Any
        params.append(ParameterInfo(name=name, type=param_type, default=param.default))
    ret_typ = sig.return_annotation if sig.return_annotation != Parameter.empty else Any
    return params, ret_typ
