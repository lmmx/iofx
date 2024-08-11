


def process_file(input_path: FilePath, output_path: NewPath) -> None:
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        outfile.write(infile.read().upper())


# Create the function model
process_file_model = create_function_model(process_file)

# Usage
try:
    result = process_file_model(
        input_path="existing_input.txt", output_path="new_output.txt"
    )
    print("File processed successfully")
except ValueError as e:
    print(f"Effect check failed: {e}")

# Accessing the parameter and effect information
print("\nParameters:")
for param in process_file_model.parameters:
    print(f" - {param.name}, Type: {param.type}, Default: {param.default}")

print("\nEffects:")
for effect in process_file_model.effects:
    print(f" - Operation: {effect.operation}, Path Parameter: {effect.path_param}")

print(f"\nReturn type: {process_file_model.return_type}")
