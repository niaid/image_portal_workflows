from em_workflows.utils import utils
from em_workflows.file_path import log as fp_log
import os
import pytest


def test_hedwig_env() -> None:
    """
    Verify env is set corretly - a bit redundant
    """
    env = utils.get_environment()
    assert env
    hedwig_env = os.environ["HEDWIG_ENV"]
    assert hedwig_env
    assert env == hedwig_env


def test_bad_hedwig_env() -> None:
    """
    Verify that no value for HEDWIG_ENV throws exception
    """
    orig_env = os.environ["HEDWIG_ENV"]
    del os.environ["HEDWIG_ENV"]
    with pytest.raises(Exception):
        env = utils.get_environment()
    os.environ["HEDWIG_ENV"] = orig_env


def test_utils_log():
    """ Need to figure out way to verify this works: how can I access the log? which log? """
    fp_log("testing 1-2-3")
    # assert log contains "testing 1-2-3"


def test_mount_config(mock_nfs_mount):
    from em_workflows.config import Config

    # Limited checks of proj_dir and assets_dir
    env = utils.get_environment()
    proj_dir = Config.proj_dir(env=env)
    assert "image_portal_workflows" in proj_dir
    assert env in proj_dir

    assets_dir = Config.assets_dir(env=env)
    assert "image_portal_workflows" in assets_dir
    assert env in assets_dir

    assert Config.LARGE_DIM == 1024
    assert Config.SMALL_DIM == 300
    assert Config.LARGE_2D == "1024x1024"
    assert Config.SMALL_2D == "300x300"
    assert os.path.exists(Config.binvol)
    assert os.path.exists(Config.brt_binary)
    assert os.path.exists(Config.dm2mrc_loc)
    assert os.path.exists(Config.clip_loc)
    assert os.path.exists(Config.convert_loc)
    assert os.path.exists(Config.header_loc)
    assert os.path.exists(Config.mrc2tif_loc)
    assert os.path.exists(Config.newstack_loc)
    assert os.path.exists(Config.tif2mrc_loc)
    assert os.path.exists(Config.xfalign_loc)
    assert os.path.exists(Config.xftoxg_loc)


def test_lookup_dims(mock_nfs_mount):
    from em_workflows.file_path import FilePath

    input_dir = "/test/input_files/dm_inputs/"


def test_get_input_dir(mock_nfs_mount):
    my_path = utils.get_input_dir.__wrapped__(input_dir="/test/input_files/dm_inputs/")
    assert "dev" in str(my_path)
