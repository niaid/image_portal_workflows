*******************************************************
Multi-channel Microscopy Workflow (CZI/SVS/OME-TIFF)
*******************************************************

Overview:

This workflow is referred to externally by Hedwig as the **Multi-channel** pipeline. It processes multidimensional
microscopy images stored in CZI (ZEISS), SVS (Aperio), and OME-TIFF file formats. Supported image types include
immunofluorescence (IF) multi-channel images (e.g., z-stacks, time lapse, and multiposition experiments) as well as
RGB brightfield images such as H&E-stained CZI slides. The workflow converts input files to OME-NGFF zarr format,
generates neuroglancer-compatible metadata, and creates thumbnail images for visualization and downstream analysis.

Outputs:
1. OME-NGFF zarr file for each input file, including OME-XML metadata.
2. Neuroglancer metadata for each sub-image (scene/channel) in the zarr file.
3. Thumbnail JPEG image for each label sub-image.

.. list-table:: Supported Extensions for File Formats
   :header-rows: 1

   * - Format Type
     - Description
     - Extensions
   * - CZI
     - ZEISS multidimensional microscopy image (IF multi-channel and RGB H&E brightfield)
     - czi, CZI
   * - SVS
     - Aperio whole-slide image
     - svs, SVS
   * - OME-TIFF
     - Open Microscopy Environment TIFF; supports single-file and multi-file datasets with modality interpreted from embedded OME-XML metadata
     - ome.tiff, OME.TIFF, ome.tif, OME.TIF

The workflow relies on OME `Bio-Formats`_ (via the ``bioformats2raw`` tool) for reading CZI, SVS, and OME-TIFF files.
Visualization is performed with `Neuroglancer`_.

Pipeline Steps
++++++++++++++

1. **Input File to Zarr Conversion**
   - Uses ``bioformats2raw`` to convert CZI, SVS, or OME-TIFF files to `OME-NGFF`_ zarr format, preserving OME-XML metadata.
   - Output: ``.zarr`` directory for each input file.

2. **Rechunking Zarr**
   - Re-chunks the zarr structure so that multi-channel/RGB channels are not split between chunks, using
     ``zarr_rechunk`` from tomojs-pytools.

3. **Copy Zarr to Assets Directory**
   - Copies the generated zarr files to the assets directory for downstream use.

4. **Generate ImageSet and Thumbnails**
   - For each sub-image in the zarr file:
     - Determines the shader type: ``RGB`` (for H&E and other RGB images), ``Grayscale``, or ``MultiChannel``
       (for IF fluorescence images).
     - Generates neuroglancer metadata (shader, dimensions, shader parameters).
     - For label sub-images, generates a thumbnail JPEG using SimpleITK (rotated 90° for correct orientation).
     - Macro sub-images are ignored.

5. **Metadata Attachment**
   - Attaches the OME-XML metadata file location to the zarr group for developer reference and provenance.

6. **Callback and API Notification**
   - Sends results and metadata to the API callback URL if provided.

.. list-table:: Summary of Input Image Types
   :header-rows: 1

   * - Format Type
     - Description
     - Processing Information
   * - CZI (IF multi-channel)
     - Multidimensional immunofluorescence image (z-stacks, time lapse, multiposition)
     - Converted to OME-NGFF zarr; neuroglancer metadata generated with ``MultiChannel`` or ``Grayscale`` shader;
       thumbnails created for label sub-images.
   * - CZI (RGB H&E)
     - RGB brightfield whole-slide image (e.g., H&E stained tissue)
     - Converted to OME-NGFF zarr; neuroglancer metadata generated with ``RGB`` shader; thumbnails created for
       label sub-images.
   * - SVS
     - Aperio whole-slide image
     - Converted to OME-NGFF zarr; neuroglancer metadata generated; thumbnails created for label sub-images.
   * - OME-TIFF
     - Open Microscopy Environment TIFF
     - Converted to OME-NGFF zarr via ``bioformats2raw``; embedded OME-XML metadata is preserved in the output zarr; ( generally does not have a label sub-image)

.. note::
   - Macro sub-images in the input file are ignored by the workflow.
   - Label sub-images are used for thumbnail generation; the thumbnail is rotated 90° for correct orientation.
   - OME-XML metadata is attached to the output zarr for troubleshooting and provenance.
   - The shader type (``RGB``, ``Grayscale``, or ``MultiChannel``) is determined automatically from the OME-XML
     metadata of each sub-image.
   - For OME-TIFF files, ``bioformats2raw`` autodetects embedded OME-XML regardless of whether it is stored
     inline in the TIFF header or as a companion ``.ome.xml`` sidecar file. The ``ome.tiff`` / ``ome.tif``
     compound extension (case-insensitive) is used to identify OME-TIFF inputs.
   - Multi-file OME-TIFF datasets (where image planes are split across multiple TIFF files) are supported.
     Set ``single_file`` to the name of the **first** TIFF file in the series; ``bioformats2raw`` will
     locate and assemble the remaining files automatically via the embedded OME-XML file list.

.. _Bio-Formats: https://www.openmicroscopy.org/bio-formats/
.. _OME-NGFF: https://ngff.openmicroscopy.org/
.. _Neuroglancer: https://github.com/google/neuroglancer
