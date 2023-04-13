utils module
============

.. automodule:: utils
   :members:
   :undoc-members:
   :show-inheritance:

   .. autofunction:: mrc_to_movie(file_path: FilePath, root: str, asset_type: str)
   .. autofunction:: gen_prim_fps(fp_in: FilePath) -> Dict
   .. autofunction:: add_asset(prim_fp: dict, asset: dict) -> dict
   .. autofunction:: cleanup_workdir(fp: FilePath)
   .. autofunction:: run_brt(file_path: FilePath, adoc_template: str, montage: int, gold: int, focus: int, fiducialless: int, trackingMethod: int, TwoSurfaces: int, TargetNumberOfBeads: int, LocalAlignments: int, THICKNESS: int,) -> None:
   .. autofunction:: init_log(file_path: FilePath)->None:
   .. autofunction:: copy_workdirs(file_path: FilePath) -> Path:
   .. autofunction:: cp_logs_to_assets(working_dir: Path, assets_dir: Path) -> None:
   .. autofunction:: list_files(input_dir: Path, exts: List[str], single_file: str = None) -> List[Path]:
   .. autofunction:: list_dirs(input_dir_fp: Path) -> List[Path]:
   .. autofunction:: gen_output_fp(input_fp: Path, output_ext: str, working_dir: Path = None) -> Path:
   .. autofunction:: gen_output_fname(input_fp: Path, output_ext) -> Path:
   .. autofunction:: run_single_file(input_fps: List[Path], fp_to_check: str) -> List[Path]:
   .. autofunction:: get_input_dir(input_dir: str) -> Path:
   .. autofunction:: gen_fps(input_dir: Path, fps_in: List[Path]) -> List[FilePath]:
   .. autofunction:: add_assets_entry(base_elt: Dict, path: Path, asset_type: str, metadata: Dict[str, str] = None) -> Dict:
   .. autofunction:: make_assets_dir(input_dir: Path, subdir_name: Path = None) -> Path:
   .. autofunction:: copy_to_assets_dir(fp: Path, assets_dir: Path, prim_fp: Path = None) -> Path:
   .. autofunction:: send_callback_body( files_elts: List[Dict], token: str = None, callback_url: str = None,) -> None:
