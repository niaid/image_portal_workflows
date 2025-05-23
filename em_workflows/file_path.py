import datetime
import shutil
import os
from typing import List, Dict, Optional, AnyStr
from pathlib import Path
import tempfile
import subprocess

from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from em_workflows.config import Config


def log(msg: str) -> None:
    """
    Convenience method to write an INFO message to a Prefect log.
    """
    try:
        get_run_logger().info(msg)
    except MissingContextError:
        print(msg)


class FilePath:
    """
    The FilePath class is used to track the directory structure of the input and output files
    when running an image pipeline. The output _asset_dir and a temporary (fast-disk) _working_dir
    are created for each input file. These members are @properties without setters to keep them immutable,
    as should the entire class, probably. It is important that each file have its own _working_dir to
    avoid any collisions during the asynchronous processing of the pipeline. Very many output files
    are created in the _working_dir, but only the outputs we care about are added to the FilePath
    for copying to the _asset_dir later in the pipeline.
    An "asset" is a resource the Hedwig Web application uses. For example an asset might be an image,
    or a movie, or output of the pipeline, that the web application users care about.

    :todo: Consider making entire class immutable
    """

    def __init__(self, share_name: str, input_dir: Path, fp_in: Path) -> None:
        """
        sets up:

        - _working_dir (fast disk where IO can occur)
        - _assets_dir (slow / big disk where outputs get moved to)

        """
        # input (AKA "Projects" file path
        self.proj_dir = input_dir
        self.fp_in = fp_in
        # not a great name - used to create the subdir into which assets are put eg
        self.base = fp_in.stem
        self._working_dir = self.make_work_dir()
        self._assets_dir = self.make_assets_dir()
        self.environment = self.get_environment()
        self.proj_root = Path(Config.proj_dir(share_name=share_name))
        self.asset_root = Path(Config.assets_dir(share_name=share_name))
        self.prim_fp_elt = self.gen_prim_fp_elt()

    def __str__(self) -> str:
        return f"FilePath: proj_root:{self.proj_root}\n\
                fp_in:{self.fp_in}\n\
                prim_fp:{self.prim_fp_elt}\n\
                working_dir:{self.working_dir}\n\
                assets_dir: {self.assets_dir}."

    @property
    def assets_dir(self) -> Path:
        """
        the top level directory where results are left.

        other subdirs are attached here containing the outputs of individual files
        """
        return self._assets_dir

    @property
    def working_dir(self) -> Path:
        """
        A pathlib.Path of the temporary (high-speed) directory where the working files
        will be stored. This is a property without a setter to make it immutable.
        :return: pathlib.Path
        """

        return self._working_dir

    def get_environment(self) -> str:
        """
        The workflows can operate in one of several environments,
        named HEDWIG_ENV for historical reasons, eg prod, qa or dev.
        This function looks up that environment.
        Raises exception if no environment found.
        """
        env = os.environ.get("HEDWIG_ENV")
        if not env:
            msg = "Unable to look up HEDWIG_ENV. Should be exported set to one of: [dev, qa, prod]"
            raise RuntimeError(msg)
        return env

    def make_work_dir(self) -> Path:
        """
        a temporary dir to house all files in the form:
        {Config.tmp_dir}{fname.stem}.
        eg: /gs1/home/macmenaminpe/tmp/tmp7gcsl4on/tomogram_fname/
        Will be rm'd upon completion.
        """
        working_dir = Path(tempfile.mkdtemp(dir=f"{Config.tmp_dir}"))
        return Path(working_dir)

    def make_assets_dir(self) -> Path:
        """
        proj_dir comes in the form {mount_point}/RMLEMHedwigQA/Projects/Lab/PI/
        want to create: {mount_point}/RMLEMHedwigQA/Assets/Lab/PI/
        """
        if "Projects" not in self.proj_dir.as_posix():
            msg = f"Error: Input directory {self.proj_dir} must contain the string 'Projects'."
            raise RuntimeError(msg)
        assets_dir_as_str = self.proj_dir.as_posix().replace("/Projects", "/Assets")
        assets_dir = Path(f"{assets_dir_as_str}/{self.base}")
        assets_dir.mkdir(parents=True, exist_ok=True)
        log(f"Created Assets dir {assets_dir}")
        return assets_dir

    def copy_to_assets_dir(self, fp_to_cp: Path) -> Path:
        """
        Copy FilePath to the assets (reported output) dir

        - fp is the Path to be copied.
        - assets_dir is the root dir (the proj_dir with s/Projects/Assets/)

        """
        # :todo: I believe following comments below are out of date
        # If prim_fp is passed, assets will be copied to a subdir defined by the input
        # file name, eg:
        # copy /tmp/tmp7gcsl4on/keyMov_SARsCoV2_1.mp4
        # to
        # /mnt/ai-fas12/RMLEMHedwigQA/Assets/Lab/Pi/SARsCoV2_1/keyMov_SARsCoV2_1.mp4
        # {mount_point}/{dname}/keyMov_SARsCoV2_1.mp4
        # (note "SARsCoV2_1" in assets_dir)
        # If prim_fp is not used, no such subdir is created.
        dest = Path(f"{self.assets_dir}/{fp_to_cp.name}")
        log(f"copying {fp_to_cp} to {dest}")
        if fp_to_cp.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
            d = shutil.copytree(fp_to_cp, dest)
        else:
            d = shutil.copyfile(fp_to_cp, dest)
        return Path(d)

    def gen_output_fp(self, output_ext: str = None, out_fname: str = None) -> Path:
        """
        cat working_dir to input_fp.name, but swap the extension to output_ext
        the reason for having a working_dir default to None is sometimes the output
        dir is not the same as the input dir, and working_dir is used to define output
        in this case.
        """
        if out_fname:
            f_name = out_fname
        else:
            f_name = f"{self.fp_in.stem}{output_ext}"

        output_fp = f"{self.working_dir.as_posix()}/{f_name}"
        return Path(output_fp)

    def gen_asset(self, asset_type: str, asset_fp) -> Dict:
        """
        Construct and return an asset (dict) based on the asset "type" and FilePath
        :param asset_type: a string that details the type of output file
        :param asset_fp: the originating FilePath to "hang" the asset on
        :return: the resulting "asset" in the form of a dict
        """
        assets_fp_no_root = asset_fp.relative_to(self.asset_root)
        asset = {"type": asset_type, "path": assets_fp_no_root.as_posix()}
        return asset

    def gen_prim_fp_elt(self, exceptions_as_str: str = None) -> Dict:
        """
        creates a single primaryFilePath element, to which assets can be appended.

        :todo: Is following "todo" comment out of date?
        :todo: input_fname_b is optional, sometimes the input can be a pair of files.

        eg::

            [
             {
              "primaryFilePath": "Lab/PI/Myproject/MySession/Sample1/file_a.mrc",
              "thumbnailIndex": 0,
              "fileMetadata": null,
              "imageSet": []
             }
            ]

        """
        # TODO - update this for czi input, parse out title from OMEXML
        title = self.fp_in.stem
        primaryFilePath = self.fp_in.relative_to(self.proj_root)
        # setting to zero here, most input files will only have a single image elt.
        # will update val if czi
        thumbnailIndex = 0
        # Note: `None` are fundamental values and they are expected if no metadata exist
        # They should be changed where they need to be changed.
        fileMetadata = None
        imageMetadata = None
        assets = []
        imageSetElement = {
            "imageName": title,
            "imageMetadata": imageMetadata,
            "assets": assets,
        }
        imageSet = [imageSetElement]
        status = "success"
        message = None
        if exceptions_as_str:
            status = "error"
            message = exceptions_as_str
        return dict(
            primaryFilePath=primaryFilePath.as_posix(),
            status=status,
            message=message,
            thumbnailIndex=thumbnailIndex,
            title=title,
            fileMetadata=fileMetadata,
            imageSet=imageSet,
        )

    def copy_workdir_to_assets(self) -> Path:
        """
        - copies all of working dir to Assets dir.
        - tests to see if the destination dir exists prior to copy
        - removes work dir upon completion.
        - returns newly created dir
        """
        dir_name_as_date = datetime.datetime.now().strftime("work_dir_%I_%M%p_%B_%d_%Y")
        dest = Path(
            f"{self.assets_dir.as_posix()}/{dir_name_as_date}/{self.fp_in.stem}"
        )
        if dest.exists():
            log(f"Output assets directory already exists! removing: {dest}")
            shutil.rmtree(dest)
        shutil.copytree(self.working_dir, dest)
        return dest

    def copy_workdir_logs_to_assets(self) -> Path:
        """
        - copies all working dir logs to Assets dir.
        - tests to see if the destination dir exists prior to copy
        - removes work dir upon completion.
        - returns newly created dir
        """
        dir_name_as_date = datetime.datetime.now().strftime("logs_%I_%M%p_%B_%d_%Y")
        dest = Path(
            f"{self.assets_dir.as_posix()}/{dir_name_as_date}/{self.fp_in.stem}"
        )
        if dest.exists():
            log(f"Output already exists! removing: {dest}")
            if dest.is_dir():
                shutil.rmtree(dest)
            else:
                dest.unlink()
        else:
            log(f"Output assets log directory: creating... {dest}")
            os.makedirs(dest, exist_ok=True)
        for f in self.working_dir.glob("*.log"):
            log(f"{f} --> {dest}")
            shutil.copy(f, dest)
        return dest

    def rm_workdir(self):
        """Removes the the entire working directory"""
        log(f"Removing working dir: {self.working_dir}")
        shutil.rmtree(self.working_dir, ignore_errors=True)

    @staticmethod
    def run(cmd: List[str], log_file: str, env: Optional[Dict[AnyStr, AnyStr]] = None, *, copy_env: bool = True) -> int:
        """Runs a Unix command as a subprocess

        - If final returncode is not 0, raises a RuntimeError

        :param cmd: list of strings representing the command to run
        :param log_file: path to the log file to write the stdout and stderr to
        :param env: dictionary of additional environment variables to pass to the subprocess
        :param copy_env: if True, the subprocess inherits the parent's environment
        :return: the return code of the subprocess


        """

        if env is None:
            if not copy_env:
                env = {}
            # Note: if env is not and copy_env is True, the subprocess inherits the parent's environment,
            # by passing env=None
        elif copy_env:
            # merge dictionaries python 3.9+
            env = os.environ | env

        log(f"Running subprocess: {' '.join(cmd)} logfile: {log_file}")

        with (subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env) as p,
              open(log_file, 'ab') as file):
            file.write(f"Running subprocess: {' '.join(cmd)}\n".encode())

            # write the outputs line by line as they come in
            for line in p.stdout:
                file.write(line)
                log(line.decode())
            file.flush()

            if p.wait() != 0:
                raise RuntimeError(f"Failed to run command: {' '.join(cmd)}")

            return p.returncode
