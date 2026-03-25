import sys
import os

def update_astro_input(file_path, data_path):
    try:
        # Read all lines from the file
        with open(file_path, 'r') as f:
            lines = f.readlines()

        # Remove empty trailing lines to find the true 'last' line
        while lines and not lines[-1].strip():
            lines.pop()

        # Replace the last line with the new file path
        if lines:
            lines[-1] = f"file {data_path}\n"
        else:
            lines.append(f"file {data_path}\n")

        # Write it back with a clean trailing newline
        with open(file_path, 'w') as f:
            f.writelines(lines)
            f.write("\n") # The "Safety" newline for C++ parsers
            
        print(f"Successfully updated {file_path}")
        
    except Exception as e:
        print(f"Error updating file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 update_input.py <input_file_path> <new_data_path>")
        sys.exit(1)
        
    update_astro_input(sys.argv[1], sys.argv[2])
