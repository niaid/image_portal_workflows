import pytest
import os
import shutil
from pathlib import Path
from prefect.engine import signals
import tempfile

from em_workflows.utils import utils
from em_workflows.file_path import FilePath
from em_workflows.config import Config
from em_workflows.brt.config import BRTConfig
from em_workflows.dm_conversion.config import DMConfig
from em_workflows.sem_tomo.config import SEMConfig
from em_workflows.constants import LARGE_DIM, SMALL_DIM
from em_workflows.dm_conversion.constants import LARGE_2D, SMALL_2D


def test_hedwig_env() -> None:
    """
    Verify env is set corretly - a bit simplistic
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
        utils.get_environment()
    os.environ["HEDWIG_ENV"] = orig_env


def test_utils_log(caplog):
    """
    Verify that utils.log() is successfully outputing prefect log messages
    :todo:  Add test within a context - will need a Flow
    """
    utils.log("utils.log test123 info message")
    assert "prefect:utils.py" in caplog.text
    assert "utils.log test123" in caplog.text


def test_mount_config(mock_nfs_mount):
    """
    Limited checks of Config constants
    :todo: Rewrite using @pytest.mark.parametrize to limit repetition
    """
    proj_dir = Config.proj_dir("test")
    assert "image_portal_workflows" in proj_dir
    #    assert env in proj_dir    # Not true in GitHub Actions test

    assets_dir = Config.assets_dir(share_name="/mocked")
    assert "image_portal_workflows" in assets_dir
    #    assert env in assets_dir    # Not true in GitHub Actions test

    assert "em_workflows" in str(Config.repo_dir)
    assert "templates" in str(Config.template_dir)

    assert LARGE_DIM == 1024
    assert SMALL_DIM == 300
    assert LARGE_2D == "1024x1024"
    assert SMALL_2D == "300x300"
    assert os.path.exists(BRTConfig.binvol)
    assert os.path.exists(Config.bioformats2raw)
    assert os.path.exists(Config.brt_binary)
    assert os.path.exists(DMConfig.dm2mrc_loc)
    assert os.path.exists(BRTConfig.clip_loc)
    # assert os.path.exists(SEMConfig.convert_loc) # uses gm instead (graphicsmagick)
    assert os.path.exists(Config.header_loc)
    assert os.path.exists(Config.mrc2tif_loc)
    assert os.path.exists(Config.newstack_loc)
    assert os.path.exists(SEMConfig.tif2mrc_loc)
    assert os.path.exists(SEMConfig.xfalign_loc)
    assert os.path.exists(SEMConfig.xftoxg_loc)


def test_share_name(mock_nfs_mount):
    """
    Check the share mapping
    """
    env = utils.get_environment()
    assert env.lower() in Config._share_name(env).lower()
    assert "rmlemhedwig" in Config._share_name(env).lower()


def test_bad_share_name(mock_nfs_mount):
    """
    Verify that bad or non-existant value raises error
    """
    with pytest.raises(Exception):
        Config._share_name("BAD")
    with pytest.raises(Exception):
        Config._share_name(None)


def test_lookup_dims(mock_nfs_mount):
    """
    Test on a number of different file types
    :todo: Consider rewriting this test to use @pytest.mark.parametrize to limit repetition
    """
    proj_dir = Config.proj_dir("test")
    input_dir = "test/input_files/dm_inputs/Projects/Lab/PI/"
    image_path = Path(os.path.join(proj_dir, input_dir, "1-As-70-007.tif"))
    dims = utils.lookup_dims(fp=image_path)
    assert dims.x == 3296 and dims.y == 2698 and dims.z == 1
    image_path = Path(os.path.join(proj_dir, input_dir, "20210525_1416.dm4"))
    dims = utils.lookup_dims(fp=image_path)
    assert dims.x == 3842 and dims.y == 4095 and dims.z == 1

    # Only run this test if the image exists; brt_inputs not in repo
    mrc_dir = "test/input_files/brt_inputs/"
    mrc_image = os.path.join(proj_dir, mrc_dir, "2013-1220-dA30_5-BSC-1_10.mrc")
    if os.path.exists(mrc_image):
        dims = utils.lookup_dims(fp=Path(mrc_image))
        assert dims.x == 2048 and dims.y == 2048 and dims.z == 121


def test_bad_lookup_dims(mock_nfs_mount):
    """
    This test should fail ``header`` doesn't work on PNG
    """
    proj_dir = Config.proj_dir("test")
    input_dir = "test/input_files/dm_inputs/Projects/Lab/PI/"
    # Error case - PNG not valid input
    image_path = Path(
        os.path.join(proj_dir, input_dir, "P6_J130_fsc_iteration_001.png")
    )
    with pytest.raises(signals.FAIL) as fail_msg:
        utils.lookup_dims(fp=image_path)
    assert "Could not open" in str(fail_msg.value)


def test_get_input_dir(mock_nfs_mount):
    utils.get_environment()
    input_dir = "/test/input_files/dm_inputs"
    my_path = utils.get_input_dir.__wrapped__(input_dir=input_dir)
    assert "image_portal_workflows" in str(my_path)
    assert input_dir in str(my_path)


def test_update_adoc(mock_nfs_mount):
    """
    Test successful modification of adoc based on a template
    :todo: consider parameterizing this to test many values
    """
    adoc_file = "plastic_brt"
    montage = 0
    gold = 15
    focus = 0
    fiducialless = 1
    trackingMethod = None
    TwoSurfaces = 0
    TargetNumberOfBeads = 20
    LocalAlignments = 0
    THICKNESS = 30

    with tempfile.TemporaryDirectory() as tmp_dir:
        adoc_tmplt = Path(os.path.join(Config.template_dir, f"{adoc_file}.adoc"))
        copied_tmplt = Path(tmp_dir) / f"{adoc_file}.adoc"
        shutil.copy(adoc_tmplt, copied_tmplt)

        env = utils.get_environment()
        mrc_image = "test/input_files/brt_inputs/2013-1220-dA30_5-BSC-1_10.mrc"
        mrc_file = Path(os.path.join(Config.proj_dir(env), mrc_image))

        updated_adoc = utils.update_adoc(
            adoc_fp=copied_tmplt,
            tg_fp=mrc_file,
            montage=montage,
            gold=gold,
            focus=focus,
            fiducialless=fiducialless,
            trackingMethod=trackingMethod,
            TwoSurfaces=TwoSurfaces,
            TargetNumberOfBeads=TargetNumberOfBeads,
            LocalAlignments=LocalAlignments,
            THICKNESS=THICKNESS,
        )

        assert updated_adoc.exists()
        assert copied_tmplt.exists()


def test_update_adoc_bad_surfaces(mock_nfs_mount):
    adoc_file = "plastic_brt"
    montage = 0
    gold = 15
    focus = 0
    fiducialless = 1
    trackingMethod = None
    # NOTE: This value is invalid
    TwoSurfaces = 2
    TargetNumberOfBeads = 20
    LocalAlignments = 0
    THICKNESS = 30

    with tempfile.TemporaryDirectory() as tmp_dir:
        adoc_tmplt = Path(os.path.join(Config.template_dir, f"{adoc_file}.adoc"))
        copied_tmplt = Path(tmp_dir) / f"{adoc_file}.adoc"
        shutil.copy(adoc_tmplt, copied_tmplt)

        env = utils.get_environment()
        mrc_image = "test/input_files/brt_inputs/2013-1220-dA30_5-BSC-1_10.mrc"
        mrc_file = Path(os.path.join(Config.proj_dir(env), mrc_image))

        with pytest.raises(signals.FAIL) as fail_msg:
            utils.update_adoc(
                adoc_fp=copied_tmplt,
                tg_fp=mrc_file,
                montage=montage,
                gold=gold,
                focus=focus,
                fiducialless=fiducialless,
                trackingMethod=trackingMethod,
                TwoSurfaces=TwoSurfaces,
                TargetNumberOfBeads=TargetNumberOfBeads,
                LocalAlignments=LocalAlignments,
                THICKNESS=THICKNESS,
            )
        assert "Unable to resolve SurfacesToAnalyze" in str(fail_msg.value)


@pytest.mark.slow
@pytest.mark.localdata
def test_mrc_to_movie(mock_nfs_mount):
    """
    - NOTE: this test depends on a sem_inputs/Projects/mrc_movie_test directory
    containing an "adjusted.mrc" file. It has to be in a "Projects" directory or
    ``FilePath.make_assets_dir()`` will fail in the FilePath constructor
    - NOTE: this test takes a relatively long time to run.
    :todo: Determine method for storing test data; smaller test images would be helpful
    as current mrc is 1.5 GB.
    """
    proj_dir = Config.proj_dir("test")
    input_dir = "test/input_files/sem_inputs/Projects/mrc_movie_test"
    input_path = Path(os.path.join(proj_dir, input_dir))
    # FIXME input directory `sem_inputs` in test/input_files is missing
    assert os.path.exists(input_path)
    # FIXME adjusted.mrc is missing
    image_path = Path(os.path.join(proj_dir, input_dir, "adjusted.mrc"))
    assert image_path.exists()

    mrc_filepath = FilePath(share_name="Test", input_dir=input_path, fp_in=image_path)
    shutil.copy(image_path, mrc_filepath.working_dir)

    #    mrc_list = utils.gen_fps.__wrapped__(input_path, [image_path])
    asset = utils.mrc_to_movie.__wrapped__(mrc_filepath, "adjusted", "recMovie")
    assert type(asset) == dict
    assert "adjusted_recMovie.mp4" in asset["path"]


def test_copy_template(mock_nfs_mount):
    """
    Tests that adoc template get copied to working directory
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        utils.copy_template(working_dir=tmp_dir, template_name="plastic_brt")
        utils.copy_template(working_dir=tmp_dir, template_name="cryo_brt")
        tmp_path = Path(tmp_dir)
        assert tmp_path.exists()
        assert Path(tmp_path / "plastic_brt.adoc").exists()
        assert Path(tmp_path / "cryo_brt.adoc").exists()


def test_copy_template_missing(mock_nfs_mount):
    """
    Tests that adoc template get copied to working directory
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(FileNotFoundError) as fnfe:
            utils.copy_template(working_dir=tmp_dir, template_name="no_such_tmplt")
        assert "no_such_tmplt" in str(fnfe.value)


def test_copy_workdirs_small(mock_nfs_mount):
    """
    Tests that the workdir is copied to the assets dir. This uses toy data only.
    """
    proj_dir = Config.proj_dir("test")
    test_dir = "test/input_files/dm_inputs/Projects/Lab/PI/"
    test_image = "P6_J130_fsc_iteration_001.png"

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = Path(tmp_dir) / "Projects"
        image_path = Path(proj_dir) / test_dir / test_image
        image_filepath = FilePath(share_name="", input_dir=input_path, fp_in=image_path)
        shutil.copy(image_path, image_filepath.working_dir)

        workdest = utils.copy_workdirs.__wrapped__(image_filepath)
        assert workdest.exists()
        assert (workdest / test_image).exists()
        assert image_path.stem in str(workdest)
        assert "work_dir" in str(workdest.parent)
