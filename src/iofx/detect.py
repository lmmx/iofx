from typing import Any, Generic, Literal, ParamSpec, TypeVar
from collections.abc import Callable
from pydantic import BaseModel, Field, model_validator, TypeAdapter, ValidationError
from pydantic.types import NewPath, FilePath
from inspect import signature
from pathlib import Path

from .param import ParameterInfo, extract_function_info

__all__ = ("FileEffect", "detect_io_effects", "FunctionModel", "create_function_model")

P = ParamSpec("P")
R = TypeVar("R")


class FileEffect(BaseModel):
    operation: Literal["read", "write", "append"]
    param: str


def detect_io_effects(parameters: list[ParameterInfo]) -> list[FileEffect]:
    effect_map = {
        FilePath: "read",
        NewPath: "write",
        Path: "append",
    }
    return [
        FileEffect(operation=effect_map[param.type], param=param.name)
        for param in parameters
        if param.type in effect_map
    ]


class FunctionModel(BaseModel, Generic[P, R]):
    func: Callable[P, R]
    parameters: list[ParameterInfo] = Field(default_factory=list)
    return_type: Any = None
    effects: list[FileEffect] = Field(default_factory=list)

    @model_validator(mode="after")
    def populate_function_info(self) -> "FunctionModel":
        self.parameters, self.return_type = extract_function_info(self.func)
        self.effects = detect_io_effects(self.parameters)
        return self

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        self.check_effects(*args, **kwargs)
        return self.func(*args, **kwargs)

    def check_effects(self, *args: P.args, **kwargs: P.kwargs):
        sig = signature(self.func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        for effect in self.effects:
            if effect.param not in bound_args.arguments:
                raise ValueError(
                    f"Parameter {effect.param} not found in function arguments",
                )

            path = bound_args.arguments[effect.param]
            if effect.operation == "read":
                try:
                    TypeAdapter(FilePath).validate_python(path)
                except ValidationError:
                    raise ValueError(f"Cannot read from non-existent file {path}")
            elif effect.operation == "write":
                try:
                    TypeAdapter(NewPath).validate_python(path)
                except ValidationError:
                    raise ValueError(f"Cannot write to existing file {path}")


def create_function_model(func: Callable[P, R]) -> FunctionModel[P, R]:
    return FunctionModel(func=func)
