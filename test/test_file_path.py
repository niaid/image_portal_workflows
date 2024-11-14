import os

from em_workflows.file_path import FilePath
from pathlib import Path
import sys


def test_gen_output_fp(mock_nfs_mount, tmp_path):
    input_dir = Path("test/input_files/brt/Projects/2013-1220-dA30_5-BSC-1_10.mrc").absolute()

    file_base_name = input_dir.stem

    fp_in = FilePath(share_name="test", input_dir=input_dir.parent, fp_in=input_dir)

    zarr = fp_in.gen_output_fp(output_ext=".zarr")
    rec_mrc = fp_in.gen_output_fp(output_ext="_rec.mrc")
    base_mrc = fp_in.gen_output_fp(output_ext=".mrc", out_fname="adjusted.mrc")
    input_mrc = fp_in.fp_in
    print(f"{rec_mrc=}\n{base_mrc=}\n{input_mrc=}")
    assert zarr.name == f"{file_base_name}.zarr"
    assert base_mrc.name == "adjusted.mrc"
    assert input_mrc.name == f"{file_base_name}.mrc"
    # Below is simply a temporary outfile loc
    assert rec_mrc.name == f"{file_base_name}_rec.mrc"


# Test the FilePath.run function by executing the python shell, and producing putput to stderr and stdout
def test_filepath_run(mock_nfs_mount, tmp_path, request):
    current_test_name = request.node.name
    log_file = tmp_path/f"{current_test_name}.log"

    input_filename = "test/input_files/dm_inputs/Projects/Lab/PI/PrP - Protein.007.tif"
    input_dir = Path(input_filename).absolute()

    fp_in = FilePath(share_name="test", input_dir=input_dir.parent, fp_in=input_dir)

    # Run the python shell, and produce output to stderr and stdout
    cmd = [sys.executable, "-c", "import sys; print('stdout'); print('stderr', file=sys.stderr)"]
    fp_in.run(cmd, log_file=str(log_file))

    # Check if the log file was created
    assert log_file.exists()

    # Check that the log contain "stdout" and "stderr" strings
    with open(log_file, "r") as f:
        log_content = f.read()
        assert "stdout" in log_content
        assert "stderr" in log_content


# Write a pytest for FilePath.run function by executing the python shell which print the environment variables.
# Combinations of setting the "env" parameter and "copy_env" need to be tested.
def test_filepath_run_env(mock_nfs_mount, tmp_path, request):
    current_test_name = request.node.name
    log_file = tmp_path/f"{current_test_name}.log"

    input_filename = "test/input_files/dm_inputs/Projects/Lab/PI/PrP - Protein.007.tif"
    input_dir = Path(input_filename).absolute()

    fp_in = FilePath(share_name="test", input_dir=input_dir.parent, fp_in=input_dir)

    os.environ["PARENT_ENV"] = "parent_env_value"

    # Case 1: env is set, and copy_env is set to False

    # Run the python shell, and print all environment variables
    cmd = [sys.executable, "-c", "import os; print(os.environ)"]
    fp_in.run(cmd, log_file=str(log_file), env={"ENV_VAR": "env_var_value"}, copy_env=False)

    # Check if the log file was created
    assert log_file.exists()

    with open(log_file, "r") as f:
        log_content = f.read()
        assert "env_var_value" in log_content
        assert "parent_env_value" not in log_content
    log_file.unlink()

    # Case 2: env is set, and copy_env is set to True

    # Run the python shell, and print all environment variables
    cmd = [sys.executable, "-c", "import os; print(os.environ)"]
    fp_in.run(cmd, log_file=str(log_file), env={"ENV_VAR": "env_var_value"}, copy_env=True)

    # Check if the log file was created
    assert log_file.exists()

    # Check that the log contain "env_var_value" string
    with open(log_file, "r") as f:
        log_content = f.read()
        assert "env_var_value" in log_content
        assert "parent_env_value" in log_content
    log_file.unlink()

    # Case 3: env is not set, and copy_env is set to False

    # Run the python shell, and print all environment variables
    cmd = [sys.executable, "-c", "import os; print(os.environ)"]
    fp_in.run(cmd, log_file=str(log_file), env=None, copy_env=True)

    # Check if the log file was created
    assert log_file.exists()

    # Check that the log contain "env_var_value" string
    with open(log_file, "r") as f:
        log_content = f.read()
        assert "env_var_value" not in log_content
        assert "parent_env_value" in log_content
    log_file.unlink()

    # Case 4: env is not set, and copy_env is set to False

    # Run the python shell, and print all environment variables
    cmd = [sys.executable, "-c", "import os; print(os.environ)"]
    fp_in.run(cmd, log_file=str(log_file), env=None, copy_env=False)

    # Check if the log file was created
    assert log_file.exists()

    # Check that the log contain "env_var_value" string
    with open(log_file, "r") as f:
        log_content = f.read()
        assert "env_var_value" not in log_content
        assert "parent_env_value" not in log_content
    log_file.unlink()
