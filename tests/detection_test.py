import pytest
from pydantic import FilePath, NewPath
from iofx import create_function_model


def process_file(input_path: FilePath, output_path: NewPath) -> None:
    with open(input_path) as infile, open(output_path, "w") as outfile:
        outfile.write(infile.read().upper())


process_file_model = create_function_model(process_file)


@pytest.fixture
def setup_files(tmp_path):
    input_file = tmp_path / "existing_input.txt"
    output_file = tmp_path / "new_output.txt"
    return input_file, output_file


def test_nonexistent_input_file(setup_files):
    input_file, output_file = setup_files

    with pytest.raises(ValueError, match="Cannot read from non-existent file"):
        process_file_model(input_path=input_file, output_path=output_file)


def test_successful_processing(setup_files):
    input_file, output_file = setup_files

    # Create the input file
    input_file.write_text("hello world")

    process_file_model(input_path=input_file, output_path=output_file)

    assert output_file.read_text() == "HELLO WORLD"


def test_existing_output_file(setup_files):
    input_file, output_file = setup_files

    # Create both input and output files
    input_file.write_text("hello world")
    output_file.touch()

    with pytest.raises(ValueError, match="Cannot write to existing file"):
        process_file_model(input_path=input_file, output_path=output_file)


def test_parameter_info():
    params = process_file_model.parameters
    assert len(params) == 2
    assert params[0].name == "input_path"
    assert params[0].type == FilePath
    assert params[1].name == "output_path"
    assert params[1].type == NewPath


def test_effect_info():
    effects = process_file_model.effects
    assert len(effects) == 2
    assert effects[0].operation == "read"
    assert effects[0].param == "input_path"
    assert effects[1].operation == "write"
    assert effects[1].param == "output_path"


def test_return_type():
    assert process_file_model.return_type is None
