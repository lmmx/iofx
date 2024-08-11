from typing import Any, Callable, Generic, Literal, ParamSpec, TypeVar
from pydantic import BaseModel, Field, model_validator, TypeAdapter, NewPath, FilePath
from inspect import signature, Parameter
from pathlib import Path

P = ParamSpec("P")
R = TypeVar("R")


class FileEffect(BaseModel):
    operation: Literal["read", "write", "append"]
    path_param: str


class ParameterInfo(BaseModel):
    name: str
    type: Any
    default: Any = Field(default=Parameter.empty)


class FunctionModel(BaseModel, Generic[P, R]):
    func: Callable[P, R]
    parameters: list[ParameterInfo] = Field(default_factory=list)
    return_type: Any = None
    effects: list[FileEffect] = Field(default_factory=list)

    @model_validator(mode="after")
    def populate_function_info(self) -> "FunctionModel":
        sig = signature(self.func)
        self.parameters = []
        for name, param in sig.parameters.items():
            param_type = (
                param.annotation if param.annotation != Parameter.empty else Any
            )
            self.parameters.append(
                ParameterInfo(
                    name=name,
                    type=param_type,
                    default=param.default,
                )
            )

            # Automatically detect file effects based on parameter types
            if param_type == FilePath:
                self.effects.append(FileEffect(operation="read", path_param=name))
            elif param_type == NewPath:
                self.effects.append(FileEffect(operation="write", path_param=name))
            elif param_type is Path:
                self.effects.append(FileEffect(operation="append", path_param=name))

        self.return_type = (
            sig.return_annotation if sig.return_annotation != Parameter.empty else Any
        )
        return self

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        self.check_effects(*args, **kwargs)
        return self.func(*args, **kwargs)

    def check_effects(self, *args: P.args, **kwargs: P.kwargs):
        sig = signature(self.func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        for effect in self.effects:
            if effect.path_param not in bound_args.arguments:
                raise ValueError(
                    f"Parameter {effect.path_param} not found in function arguments"
                )

            path = bound_args.arguments[effect.path_param]
            if effect.operation == "read":
                try:
                    TypeAdapter(FilePath).validate_python(path)
                except:
                    raise ValueError(f"Cannot read from non-existent file {path}")
            elif effect.operation == "write":
                try:
                    TypeAdapter(NewPath).validate_python(path)
                except:
                    raise ValueError(f"Cannot write to existing file {path}")


def create_function_model(func: Callable[P, R]) -> FunctionModel[P, R]:
    return FunctionModel(func=func)
