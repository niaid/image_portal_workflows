*************
Large 2D RGB Workflow
*************

Overview:

This workflow is referred to externally by Hedwig as the **Flat 2D** pipeline. It processes large 2D RGB images
(PNG or TIFF) to produce an OME-NGFF zarr pyramid suitable for `Neuroglancer`_ visualization, along with a thumbnail
and a key image for browsing. The pipeline is intended for any large 2D image that requires Neuroglancer
visualization, including annotated micrographs, color composites, and other 2D figures.

Outputs:
 1. OME-NGFF zarr pyramid (neuroglancerZarr asset) for Neuroglancer visualization, including neuroglancer metadata.
 2. Thumbnail JPEG image — maximum 300 × 300 pixels, aspect ratio preserved.
 3. Key image JPEG — maximum 1024 × 1024 pixels, aspect ratio preserved.

.. list-table:: Supported Extensions for File Formats
   :header-rows: 1

   * - Format Type
     - Description
     - Extensions
   * - PNG
     - Portable Network Graphics
     - png, PNG
   * - TIFF
     - Tag Image File Format
     - tif, TIF, tiff, TIFF

The workflow uses `ImageMagick`_ for input normalization, OME `Bio-Formats`_ (via ``bioformats2raw``) for zarr
conversion, ``zarr_rechunk`` from tomojs-pytools for rechunking, and `SimpleITK`_ for thumbnail and key image
generation. Visualization is performed with `Neuroglancer`_.

Pipeline Steps
++++++++++++++

1. **Input Normalization to TIFF**
   - Uses ImageMagick ``convert`` to produce a normalized TIFF from the input PNG or TIFF file.
   - Alpha transparency is removed and replaced with a white background.
   - Tile geometry is set to 128 × 128 pixels for downstream processing optimization.
   - Output: ``.tiff`` file in the working directory.

2. **TIFF to Zarr Conversion**
   - Uses ``bioformats2raw`` to convert the normalized TIFF to an `OME-NGFF`_ zarr pyramid.
   - Output: ``.zarr`` directory in the working directory.

3. **Rechunking Zarr**
   - Re-chunks the zarr structure so that multi-channel/RGB channels are not split between chunks, using
     ``zarr_rechunk`` from tomojs-pytools.

4. **Copy Zarr to Assets Directory**
   - Copies the rechunked zarr to the assets directory for downstream access.

5. **Generate Neuroglancer Asset**
   - Reads the zarr from the working directory to compute metadata.
   - Determines the shader type (``RGB``, ``Grayscale``, or ``MultiChannel``) automatically from the image data.
   - Produces a ``neuroglancerZarr`` asset pointing to the ``0`` (full-resolution) level of the zarr in the
     assets directory, along with neuroglancer metadata (shader, dimensions, shaderParameters).

   .. note::
      The ``dimensions`` field is currently hardcoded to ``XY`` as the GUI does not accept ``XYC`` for this
      pipeline.

6. **Generate Thumbnail and Key Image**
   - Uses `SimpleITK`_ to extract and resize the image from the working zarr.
   - Thumbnail: maximum 300 × 300 pixels, written as JPEG (quality 90).
   - Key image: maximum 1024 × 1024 pixels, written as JPEG (quality 90).
   - Both images are copied to the assets directory.

7. **Callback and API Notification**
   - Sends results and metadata to the API callback URL if provided.

.. list-table:: Summary of Input Image Types
   :header-rows: 1

   * - Format Type
     - Description
     - Processing Information
   * - PNG
     - Color or grayscale PNG image, possibly with alpha transparency
     - Normalized to TIFF (alpha removed, white background); converted to OME-NGFF zarr;
       neuroglancer metadata and thumbnail/key image generated.
   * - TIFF
     - Color or grayscale TIFF image
     - Normalized to TIFF (alpha removed, white background); converted to OME-NGFF zarr;
       neuroglancer metadata and thumbnail/key image generated.

.. note::
   - All inputs pass through the TIFF normalization step regardless of format. This ensures a consistent
     tiled TIFF with alpha removed for reliable zarr conversion.
   - The shader type (``RGB``, ``Grayscale``, or ``MultiChannel``) is determined automatically from the image data.
   - Thumbnail and key image dimensions preserve the aspect ratio and will be at most the stated maximum size.

.. _Bio-Formats: https://www.openmicroscopy.org/bio-formats/
.. _OME-NGFF: https://ngff.openmicroscopy.org/
.. _Neuroglancer: https://github.com/google/neuroglancer
.. _ImageMagick: https://imagemagick.org/
.. _SimpleITK: https://simpleitk.readthedocs.io/
