import os
import pytest
import sys
import shutil
import time
import tempfile
from pathlib import Path
from unittest.mock import Mock
import json

from prefect import flow, task, allow_failure
from prefect.filesystems import LocalFileSystem
from prefect.serializers import PickleSerializer
from prefect.states import State
from prefect.settings import PREFECT_HOME

from em_workflows.utils import utils
from em_workflows.file_path import FilePath
from em_workflows.config import Config
from em_workflows.brt.config import BRTConfig
from em_workflows.dm_conversion.config import DMConfig
from em_workflows.sem_tomo.config import SEMConfig
from em_workflows.constants import LARGE_DIM, SMALL_DIM
from em_workflows.dm_conversion.constants import LARGE_2D, SMALL_2D, VALID_2D_INPUT_EXTS


def test_bio2r_environ(mock_nfs_mount, caplog):
    from em_workflows.config import Config
    from em_workflows.file_path import FilePath
    import os
    import tempfile

    assert os.path.exists(Config.bioformats2raw)
    with tempfile.NamedTemporaryFile() as logfile:
        cmd = [Config.bioformats2raw, "--version"]
        out = FilePath.run(cmd=cmd, log_file=logfile.name)
        assert out == 0, "Command output is not success code"
        print(logfile.name)
        assert os.path.exists(logfile.name)
        with open(logfile.name, "r") as _file:
            assert "Bio-Formats version" in _file.read()


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
    """
    from prefect import flow

    @flow
    def my_flow():
        utils.log("utils.log test123 info message")

    my_flow()

    assert "prefect.flow_runs:utils.py" in caplog.text
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


@pytest.mark.parametrize(
    "input_dir, filename, x, y, z",
    [
        (
            "test/input_files/dm_inputs/Projects/Lab/PI/",
            "1-As-70-007.tif",
            3296,
            2698,
            1,
        ),
        (
            "test/input_files/dm_inputs/Projects/Lab/PI/",
            "20210525_1416.dm4",
            3842,
            4095,
            1,
        ),
        (
            "test/input_files/brt_inputs/Projects/",
            "2013-1220-dA30_5-BSC-1_10.mrc",
            2048,
            2048,
            121,
        ),
    ],
)
def test_lookup_dims(mock_nfs_mount, input_dir, filename, x, y, z):
    """
    Test on a number of different file types
    """
    # Only run this test if the image exists; brt_inputs not in repo
    proj_dir = Config.proj_dir("test")
    image_path = Path(os.path.join(proj_dir, input_dir, filename))
    if not image_path.exists():
        pytest.skip()
    dims = utils.lookup_dims(fp=image_path)
    assert dims.x == x and dims.y == y and dims.z == z


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
    with pytest.raises(RuntimeError) as fail_msg:
        utils.lookup_dims(fp=image_path)
    assert "Could not open" in str(fail_msg.value)


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

        with pytest.raises(ValueError) as fail_msg:
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
    if not input_path.exists():
        pytest.skip(f"{input_path} does not exist.")
    # FIXME input directory `sem_inputs` in test/input_files is missing
    assert os.path.exists(input_path)
    # FIXME adjusted.mrc is missing
    image_path = Path(os.path.join(proj_dir, input_dir, "adjusted.mrc"))
    if not image_path.exists():
        pytest.skip(f"Image {image_path.name} not found")

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


def test_list_files(mock_nfs_mount):
    """
    Tests list files only gets one file if it has x_file_name argument
    """
    input_dir = Path("test/input_files/dm_inputs/Projects/Lab/PI")
    exts = VALID_2D_INPUT_EXTS
    x_file_name = "20210525_1416_A000_G000.dm4"

    files = utils.list_files.__wrapped__(input_dir=input_dir, exts=exts)
    assert len(files) > 1

    files = utils.list_files.__wrapped__(
        input_dir=input_dir, exts=exts, single_file=x_file_name
    )
    assert len(files) == 1


def test_state_hooks_calls(prefect_test_fixture):
    """
    Tests whether states hooks are called appropriately
    """
    my_hook = Mock(return_value=None)
    # Kept getting error: AttributeError `__name__`
    my_hook.__name__ = "foo"

    @task
    def my_task(param1, param2):
        print(param1, param2)

    @flow(
        name="hooks_test",
        on_completion=[my_hook],
        on_failure=[my_hook],
    )
    def my_flow(param1: int, param2: int):
        my_task.submit(param1, param2)

    params = dict(param1=1, param2=2)
    my_flow(**params)

    my_hook.assert_called_once()
    print(my_hook.call_args)
    # the call args are sent as (flow, flow_run, state)
    # in case of unitest, they are received in args and kwargs
    _, kw = my_hook.call_args
    assert "flow" in kw
    assert "flow_run" in kw
    flow_run = kw["flow_run"]
    assert "state" in kw
    state = kw["state"]

    assert flow_run.parameters == params
    assert flow_run.context == {}
    assert str(state.type) == "StateType.COMPLETED"


def test_message_store_using_local_storage_via_task(
    prefect_test_fixture,
):
    """
    Tests whether task hooks can store strings in local storage
    that can be retrieved later properly
    """

    def task_hook(task, task_run, state):
        message = f"Failure in pipeline step: {task.name}"
        map_idx = task_run.name.split("-")[-1]
        flow_run_id = state.state_details.flow_run_id
        path = f"{flow_run_id}__{map_idx}"
        try:
            Config.local_storage.read_path(path)
        except ValueError:  # path not found
            Config.local_storage.write_path(path, message.encode())
            print(f"Written error messge to {path=}")

    @task(
        name="my-task",
        on_failure=[task_hook],
    )
    def fails_in_1(arg: int):
        if arg == 1:
            raise ValueError("Failing")
        return True

    @task
    def valid(arg: int):
        return arg

    @flow(name="hooks_test")
    def my_flow():
        lst = [0, 1, 2]
        a = valid.map(lst)
        b = fails_in_1.map(a)
        c = valid.map(b)
        result = list()
        for idx, item in enumerate(c):
            state: State = item.wait()
            if state.is_completed():
                result.append(item.result())
            else:
                path = f"{state.state_details.flow_run_id}__{idx}"
                try:
                    message = Config.local_storage.read_path(path)
                    result.append(message.decode())
                except ValueError:
                    result.append("Something went wrong.")
        return result

    result = my_flow()
    assert isinstance(result, list)
    assert result[0] is True
    assert result[1] == "Failure in pipeline step: my-task"


def test_downstream_runs_if_upstream_fails_with_allow_failure_annotation(
    prefect_test_fixture,
):
    @task
    def fails_on_two(arg):
        if arg == 2:
            raise ValueError("Fail task!")
        return arg

    @task
    def bar(y):
        return y

    @flow
    def test_flow():
        f = fails_on_two.map([1, 2, 3])
        b = bar.submit(2, wait_for=[allow_failure(f)])
        return b

    flow_state = test_flow()
    assert flow_state == 2


def test_prefect_future_wait_with_valid_results_actually_waits(
    prefect_test_fixture,
):
    @task
    def long_running_on_1(arg):
        if arg == 1:
            time.sleep(10)
        print(f"Passed long on {arg=}")
        return arg

    @task
    def short_running(arg):
        print(f"Passes short on {arg=}")
        return arg

    @flow
    def test_flow():
        a = [1, 2, 3]
        longs = long_running_on_1.map(a)
        shorts = short_running.map(longs)
        for arg, futures in zip(a, shorts):
            state = futures.wait()
            result = state.result()
            assert arg == result

    flow_state = test_flow()
    for state in flow_state:
        assert state.is_completed(), f"Item {state=}"


class TestResult:
    def __init__(self, x=1):
        self.x = x


def test_task_result_is_persisted_and_accessible_at_the_end(
    prefect_test_fixture,
):
    # Ref: https://github.com/PrefectHQ/prefect/blob/
    # e748ad325e04ba4d0b5d472fef746d33518fe94e/tests/results/test_task_results.py#L301
    storage = LocalFileSystem(basepath=PREFECT_HOME.value() / "test-storage")
    serializer = PickleSerializer(picklelib="pickle")

    @task(
        persist_result=True,
        result_storage=storage,
        result_serializer=serializer,
        result_storage_key="{flow_run.flow_name}__bar",
    )
    def bar(x: int = 1, y: str = "test"):
        return TestResult(x=2)

    @flow
    def foo():
        return bar(y="foo", return_state=True)

    flow_state = foo(return_state=True)
    task_state = flow_state.result()
    assert task_state.data.storage_key == "foo__bar"

    result = storage.read_path("foo__bar")
    assert isinstance(result, bytes), result
    value = serializer.loads(json.loads(result)["data"].encode())
    assert isinstance(value, TestResult)
    assert value.x == 2


def test_task_result_persistend_and_accessed_by_hooks(
    prefect_test_fixture,
):
    storage = LocalFileSystem(basepath=PREFECT_HOME.value() / "test-storage")
    serializer = PickleSerializer(picklelib="pickle")

    mock_hook = Mock(return_value=None)
    # Kept getting error: AttributeError `__name__`
    mock_hook.__name__ = "mock_hook"

    def hook(flow, flow_run, state):
        result = storage.read_path(f"{flow_run.id}__bar")

        assert isinstance(result, bytes), result
        value = serializer.loads(json.loads(result)["data"].encode())
        assert isinstance(value, TestResult)
        assert value.x == 2

    @task(
        persist_result=True,
        result_storage=storage,
        result_serializer=serializer,
        result_storage_key="{flow_run.id}__bar",
    )
    def bar(x: int = 1, y: str = "test"):
        return TestResult(x=2)

    @flow(
        on_completion=[hook, mock_hook],
    )
    def foo():
        return bar(y="foo", return_state=True)

    flow_state = foo(return_state=True)
    task_state = flow_state.result()
    assert "__bar" in task_state.data.storage_key

    # asserts that mock hook and custom hook both are called
    mock_hook.assert_called_once(), "Custom hook not called as well"


def test_class_del_via_garbage_collection(
    prefect_test_fixture,
):
    pytest.skip("Skipping; Only here to understand gc, obj.refcount, prefect")
    mock = Mock(return_value=None)
    # Kept getting error: AttributeError `__name__`
    mock.__name__ = "mock"

    class FilePath:
        def __init__(self):
            print("initializing object")
            self.name = "file_path"

        def __del__(self):
            print("calling mock and deleting")
            mock()

    @task
    def mytask():
        fp = FilePath()
        print(f"task {sys.getrefcount(fp)=}")
        return fp

    @flow
    def myflow():
        fp = mytask()
        print(f"flow {sys.getrefcount(fp)=}")
        return fp

    fp = myflow()
    assert fp.name == "file_path"
    """
    Assumption was, there would only be one reference for fp instance
    However, the way prefect works, there can be so many more (got 7 here)
    """
    print(f"Outside {sys.getrefcount(fp)=}")
    assert sys.getrefcount(fp) == 1, "More reference count than we assumed"
