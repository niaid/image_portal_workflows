BRT flow
========

.. automodule:: em_workflows.brt.flow
   :members:
   :undoc-members:
   :show-inheritance:

   .. autofunction:: gen_dimension_command(file_path: FilePath, ali_or_rec: str) -> str
   .. autofunction:: gen_ali_x(file_path: FilePath, z_dim) -> None
   .. autofunction:: gen_ali_asmbl(file_path: FilePath) -> None
   .. autofunction:: gen_mrc2tiff(file_path: FilePath) -> None
   .. autofunction:: gen_thumbs(file_path: FilePath, z_dim) -> dict
   .. autofunction:: gen_copy_keyimages(file_path: FilePath, z_dim: str) -> dict
   .. autofunction:: gen_tilt_movie(file_path: FilePath) -> dict
   .. autofunction:: gen_recon_movie(file_path: FilePath) -> dict
   .. autofunction:: gen_clip_avgs(file_path: FilePath, z_dim: str) -> None
   .. autofunction:: consolidate_ave_mrcs(file_path: FilePath) -> dict
   .. autofunction:: gen_ave_8_vol(file_path: FilePath)
   .. autofunction:: gen_ave_jpgs_from_ave_mrc(file_path: FilePath)
   .. autofunction:: cleanup_files(file_path: FilePath, pattern=str)
