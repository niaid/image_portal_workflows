CZI flow
========

.. automodule:: em_workflows.czi.flow
   :members:
   :undoc-members:

   .. autofunction:: gen_thumb(image: HedwigZarrImage, file_path: FilePath, image_name: str) -> dict
   .. autofunction:: rechunk_zarr(file_path: FilePath) -> None
   .. autofunction:: copy_zarr_to_assets_dir(file_path: FilePath) -> None
   .. autofunction:: generate_imageset(file_path: FilePath) -> List[Dict]
   .. autofunction:: generate_zarr(file_path: FilePath)
   .. autofunction:: find_thumb_idx(callback: List[Dict]) -> List[Dict]
   .. autofunction:: update_file_metadata(file_path: FilePath, callback_with_zarr: Dict) -> Dict
   .. autofunction:: generate_czi_imageset(file_path: FilePath) -> List[Dict]
