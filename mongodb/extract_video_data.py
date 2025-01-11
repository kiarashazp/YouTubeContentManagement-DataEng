import tarfile
import os


def extract_tar_to_data(tar_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    try:
        with tarfile.open(tar_file, 'r') as tar:
            tar.extractall(path=output_dir)
            print(f"Extracted {tar_file} to {output_dir}")
    except Exception as e:
        print(f"Error extracting {tar_file}: {e}")


tar_file_path = "/videos.tar.gz"  # Adjust path as needed
output_directory = "../db"  # Target directory
extract_tar_to_data(tar_file_path, output_directory)
