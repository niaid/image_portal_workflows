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
   .. autofunction:: copy_workdirs(file_path: FilePath) -> Path:
   .. autofunction:: list_files(input_dir: Path, exts: List[str], single_file: str = None) -> List[Path]:
   .. autofunction:: list_dirs(input_dir_fp: Path) -> List[Path]:
   .. autofunction:: get_input_dir(input_dir: str) -> Path:
   .. autofunction:: gen_fps(input_dir: Path, fps_in: List[Path]) -> List[FilePath]:
   .. autofunction:: send_callback_body( files_elts: List[Dict], token: str = None, callback_url: str = None,) -> None:
