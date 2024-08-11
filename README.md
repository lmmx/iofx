# iofx: Automated I/O Effect Detection for Python Functions

`iofx` is a Python library that provides automated detection and validation of file I/O effects for
functions. It leverages Pydantic for type checking and effect validation.

## Features

- Automatic detection of file read/write operations based on parameter types
- Runtime validation of file effects
- Integration with Pydantic for robust type checking
- Easy-to-use decorator for adding effect detection to functions

## Installation

```bash
pip install iofx
```

## Quick Start

```python
from iofx import create_function_model
from pydantic import FilePath, NewPath

def process_file(input_path: FilePath, output_path: NewPath) -> None:
    with open(input_path) as infile, open(output_path, "w") as outfile:
        outfile.write(infile.read().upper())

# Create the function model
process_file_model = create_function_model(process_file)

# Usage
try:
    result = process_file_model(
        input_path="existing_input.txt",
        output_path="new_output.txt",
    )
    print("File processed successfully")
except ValueError as e:
    print(f"Effect check failed: {e}")
```

## How It Works

1. `create_function_model` analyzes the function's signature and parameter types.
2. It automatically detects potential file I/O effects based on parameter annotations:
   - `FilePath`: Indicates a file read operation
   - `NewPath`: Indicates a file write operation to a new file
   - `Path`: Indicates a potential file append operation
3. At runtime, it checks if the file operations are valid (e.g., input file exists, output file
   doesn't exist for `NewPath`).

## API Reference

### `create_function_model(func: Callable[P, R]) -> FunctionModel[P, R]`

Creates a `FunctionModel` instance for the given function, which wraps the original function with
effect detection and validation.

### `class FunctionModel(BaseModel, Generic[P, R])`

- `func`: The original function
- `parameters`: List of `ParameterInfo` objects describing function parameters
- `return_type`: Return type of the function
- `effects`: List of `FileEffect` objects describing detected file I/O effects
