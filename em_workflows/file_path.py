import datetime
import shutil
import os
from typing import List, Dict
from pathlib import Path
import tempfile
import re
from prefect.engine import signals
import subprocess
from em_workflows.config import Config


class FilePath:
    def __init__(self, proj_dir: Path, fp_in: Path) -> None:
        """
        sets up:
        working dir (fast disk where IO can occur)
        assets_dir (slow / big disk where outputs get moved to)

        """
        self.proj_dir = proj_dir
        # input (AKA "Projects" file path
        self.fp_in = fp_in
        # current transformation of this file - init to input fname
        self._current = fp_in
        # self.fp_sanitized = utils.sanitize_file_name(fp_in)
        self._working_dir = self.make_work_dir()
        self._assets_dir = self.make_assets_dir()
        self.environment = self.get_environment()
        self.proj_dir = Path(Config.proj_dir(env=self.environment))
        self.asset_dir = Path(Config.assets_dir(env=self.environment))
        self.prim_fp_elt = self.gen_prim_fp_elt()

    def __repr__(self) -> str:
        return f"FilePath: proj_dir:{self.proj_dir}, \
                fp_in:{self.fp_in}, prim_fp:{self.prim_fp_elt}, working_dir:{self.working_dir} \
                assets_dir: {self.assets_dir}."

    @property
    def assets_dir(self) -> Path:
        return self._assets_dir

    @property
    def current(self) -> Path:
        return self._current

    @property
    def working_dir(self) -> Path:
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

    def update_current(self, new_fp: Path) -> None:
        self._current = new_fp

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
        if not "Projects" in self.proj_dir.as_posix():
            msg = f"Error: Input directory {self.proj_dir} must contain the string 'Projects'."
            raise signals.FAIL(msg)
        assets_dir_as_str = self.proj_dir.as_posix().replace("/Projects/", "/Assets/")
        assets_dir = Path(assets_dir_as_str)
        assets_dir.mkdir(parents=True, exist_ok=True)
        return assets_dir

    def copy_to_assets_dir(self, fp_to_cp: Path) -> Path:
        """
        Copy fp to the assets (reported output) dir
        fp is the Path to be copied.
        assets_dir is the root dir (the input_dir with s/Projects/Assets/)
        If prim_fp is passed, assets will be copied to a subdir defined by the input
        file name, eg:
        copy /tmp/tmp7gcsl4on/keyMov_SARsCoV2_1.mp4
        to
        /mnt/ai-fas12/RMLEMHedwigQA/Assets/Lab/Pi/SARsCoV2_1/keyMov_SARsCoV2_1.mp4
        {mount_point}/{dname}/keyMov_SARsCoV2_1.mp4
        (note "SARsCoV2_1" in assets_dir)
        If prim_fp is not used, no such subdir is created.
        """
        dest = Path(f"{self.assets_dir}/{fp_to_cp.name}")
        if fp_to_cp.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
            shutil.copytree(fp_to_cp, dest)
        else:
            shutil.copyfile(fp_to_cp, dest)
        return dest

    def add_assets_entry(
        self, asset_path: Path, asset_type: str, metadata: Dict[str, str] = None
    ) -> Dict:
        """
        asset type can be one of:

        averagedVolume
        keyImage
        keyThumbnail
        recMovie
        tiltMovie
        volume
        neuroglancerPrecomputed

        used to build the callback for API
        metadata is used in conjunction with neuroglancer only
        """
        valid_typs = [
            "averagedVolume",
            "keyImage",
            "thumbnail",
            "keyThumbnail",
            "recMovie",
            "tiltMovie",
            "volume",
            "neuroglancerPrecomputed",
        ]
        if asset_type not in valid_typs:
            raise ValueError(
                f"Asset type: {asset_type} is not a valid type. {valid_typs}"
            )
        fp_no_mount_point = asset_path.relative_to(Config.assets_dir(env=self.environment))
        if metadata:
            asset = {
                "type": asset_type,
                "path": fp_no_mount_point.as_posix(),
                "metadata": metadata,
            }
        else:
            asset = {"type": asset_type, "path": fp_no_mount_point.as_posix()}
        self.prim_fp_elt["assets"].append(asset)
        return asset

    def gen_output_fp(self, output_ext: str) -> Path:
        """
        cat working_dir to input_fp.name, but swap the extension to output_ext
        the reason for having a working_dir default to None is sometimes the output
        dir is not the same as the input dir, and working_dir is used to define output
        in this case.
        """
        stem_name = self.fp_in.stem
        # stem_name = re.sub(r'[()\[\] ]',"_",stem_name)
        output_fp = f"{self.working_dir.as_posix()}/{stem_name}{output_ext}"
        return Path(output_fp)

    def filter_by_suffix(self, suffixes: List[str]) -> bool:
        for ext in suffixes:
            if self.current.suffix.lower() == ext:
                return True
        return False

    def gen_prim_fp_elt(self) -> Dict:
        """
        creates a single primaryFilePath element, to which assets can be appended.
        TODO:
        input_fname_b is optional, sometimes the input can be a pair of files.
        eg:
        [
         {
          "primaryFilePath": "Lab/PI/Myproject/MySession/Sample1/file_a.mrc",
          "title": "file_a",
          "assets": []
         }
        ]
        """
        title = self.fp_in.stem
        primaryFilePath = self.fp_in.relative_to(self.proj_dir)
        return dict(
            primaryFilePath=primaryFilePath.as_posix(), title=title, assets=list()
        )

    def copy_workdir_to_assets(self):
        """copies all of working dir to Assets dir"""
        dir_name_as_date = datetime.datetime.now().strftime("work_dir_%I_%M%p_%B_%d_%Y")
        dest = Path(
            f"{self.assets_dir.as_posix()}/{dir_name_as_date}/{self.fp_in.stem}"
        )
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(self.working_dir, dest)
        return dest

    def rm_workdir(self):
        shutil.rmtree(self.working_dir)

    @staticmethod
    def run(cmd: List[str], log_file: str) -> None:
        sp = subprocess.run(cmd, check=True, capture_output=True, encoding="UTF-8")
        with open(log_file, "w+") as _file:
            _file.write(f"STDOUT:{sp.stdout}")
            _file.write(f"STDERR:{sp.stderr}")
