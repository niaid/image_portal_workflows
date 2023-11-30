DM conversion flow
==================

.. automodule:: em_workflows.dm_conversion.flow
   :members:
   :undoc-members:
   :show-inheritance:

   .. autofunction:: convert_dms_to_mrc(file_path: FilePath) -> None
   .. autofunction:: convert_if_int16_tiff(file_path: FilePath) -> None
   .. autofunction:: convert_2d_mrc_to_tiff(file_path: FilePath) -> None
   .. autofunction:: convert_dm_mrc_to_jpeg(file_path: FilePath) -> None
   .. autofunction:: scale_jpegs(file_path: FilePath, size: str) -> Optional[dict]
