import os
import logging

def check_files_different(file1, file2, file3):
    """
    Checks if three file paths are different.

    Args:
        file1 (str): The path to the first file.
        file2 (str): The path to the second file.
        file3 (str): The path to the third file.

    Raises:
        TypeError: If any of the arguments are not strings.
        FileNotFoundError: If any of the files do not exist.
        ValueError: If any of the file paths are the same.

    Returns:
        None.
    """
    # Check if all arguments are strings
    logging.info("Checking if all arguments are strings")
    if (
        not isinstance(file1, str)
        or not isinstance(file2, str)
        or not isinstance(file3, str)
    ):
        raise TypeError("All arguments must be strings")

    # Get the absolute paths of the files
    abs_file1 = os.path.abspath(file1)
    abs_file2 = os.path.abspath(file2)
    abs_file3 = os.path.abspath(file3)

    # Check if the files exist
    logging.info("Checking if the files exist")
    if not os.path.exists(abs_file1) or not os.path.exists(abs_file2):
        raise FileNotFoundError(
            "All files must exist, add file to base folder to continue."
        )
    logging.info("Awesome all files exist")

    # Check if the absolute paths are different
    logging.info("Checking if the files have the same name")
    if abs_file1 == abs_file2 or abs_file1 == abs_file3 or abs_file2 == abs_file3:
        raise ValueError("All file paths must be different")
    logging.info("File name check successful")

    # Check if support file type
    files = [abs_file1, abs_file2, abs_file3]
    return all((check_support_file_type(file) for file in files))


def check_support_file_type(file_path):
    """
    Check if a file is in CSV format.

    Args:
        file_path (str): Path to file.

    Returns:
        bool: True if file is in CSV format, False otherwise.

    Raises:
        ValueError: If file is not in CSV format.
    """
    logging.info("Checking if the file is csv file type")
    if not file_path.endswith(".csv"):
        raise ValueError(
            "Sorry, file must be in CSV format. \
                Please request an intake for more file type support."
        )
    logging.info("File type support check successful")
    return True

