******
2D Electron Microscopy Workflow (formally internally DM conversion workflow)
******

Overview:

This workflow supports generating thumbnails and key images form input images. Certain EM file formats and contents
use enhanced resampling and filtering techniques to reduce noise while adjusting the dynamic range of the images. This
workflow originally was designed to handle DM3 files from TEM or STEM microscopes but has been extended to handle
other 2D EM image file formats, including MRC and TIFF files.

Outputs:
 1. A thumbnail image for each input image.
 2. A key image for each input image.

.. list-table:: Supported Extensions for File Formats
   :header-rows: 1

   * - Format Type
     - Description
     - Extensions
   * - DM3
     -  image format used by Digital Micrograph
     - dm3
   * - DM4
     - image format used by Digital Micrograph, possibly limited to 64-bit integers
     - dm4
   * - MRC
     - image format used by the IMOD tool suite
     - mrc
   * - TIFF
     - Standard Tag Image File Format ( TIFF 6.0 ), OME TIFF not supported, TIFF stacks not supported.
     - tif, tiff
   * - PNG
     - Portable Network Graphics
     - png
   * - JPEG
     - Joint Photographic Experts Group
     - jpg, jpeg


The toolbox of `IMOD`_ is used primarily to process the EM images, along with `SimpleITK` for additional image
processing tasks.


.. list-table:: Summary of Input Image Types
   :header-rows: 1

   * - Format Type
     - Pixel Type
     - Description
     - Processing Information
   * - JPEG, PNG, TIFF
     - 8-bit RGB (palette) or RGBA
     - Images may be figures or rendered EM image of wells?.
     - Images should directly resized without intensity scaling or advanced filtering.
   * - TIFF, PNG, JPEG
     - 8-bit gray-scale
     - Images may be rendered EM images with captions or annotations.
     - Images should directly resized without intensity scaling or advanced filtering.
   * - TIFF
     - 16-bit (signed or unsigned) gray-scale
     - EM images with a high dynamic range.
     - Convert to 8-bit
       - Use IMOD's `newstack`_ tool to resample, filter, and scale intensities to 8-bit.
   * - DM4 or DM3 Images
     - 32-bit float (after MRC conversion)
     - Minimally processed EM images with a high dynamic range.
     - Convert to MRC via `dm2mrc`_ command to 32-bit float MRC files, then use newstack to resample, filter, and scale
       intensities to 8-bit TIFF.
   * - MRC (2D only)
     - 32-bit float ( no other current sample inputs )
     - Expected EM images with likely high dynamic range.
     - Convert to 8-bit TIFF via `newstack`_ command to resample, filter, and scale intensities to 8-bit.


Format Conversion
+++++++++++++++++

The first step is to convert the input to a format compatible with the IMOD tool suite (e.g., MRC or TIFF).

Inputs: DM3, DM4, MRC, or TIFF files
Outputs: MRC or TIFF files
Steps:
1. Use the `dm2mrc`_ tool to convert DM3 or DM4 files to MRC format.
2. For MRC files, no additional conversion is required.
3. For 16-bit TIFF files, convert them to 8-bit TIFF using the `newstack`_ tool.

Resampling
++++++++++

The EM images can be broadly characterized as having a low signal-to-noise ratio and a high dynamic range with potential
outliers. The resampling or resizing of these images requires advanced algorithms and options that are not available
with conventional image processing tools. Through proper resampling and filtering, the images' signal-to-noise ratio
can be improved, and the dynamic range can be reduced for better visualization.

The IMOD tool `newstack`_ is used to resample the images.

Inputs: MRC or TIFF files
Outputs: TIFF files as 8-bit images
Steps:
1. Determine the size or dimensions of the input images.
2. Compute the factor to reduce the size of the input image to the desired key image size.
3. Use the `newstack`_ tool to filter and resize/shrink the input image to approximately the desired size.

.. code-block:: bash

 newstack -format TIFF -shrink $shrink_factor -antialias 6 -mode 0 -float 1 input.tiff output.tiff

Conditionals:
  - If the input image size is less than the desired key image size, then `-shrink` and `-antialias` options are not
    used.
  - If the input pixel type is 8-bit, then the `float` option is not used.

The above command converts any supported scalar pixel input type to an 8-bit image with a dynamic range reasonable for
visualization. The `float` option is used so the resampled pixel intensities are normalized to fill the output range.

Output Generation
+++++++++++++++++

The final step is to generate the thumbnail and key images using `SimpleITK`. The compression and filtering options are
tuned for web display.

Inputs: TIFF files
Outputs: JPEG files
Steps:
1. Use `SimpleITK` to resize the TIFF files to the desired dimensions for thumbnails and key images.
2. Save the resized images as JPEG files with appropriate compression levels.

.. code-block:: python

 sitk.WriteImage(img, output_path, useCompression=True, compressionLevel=compression_level)

Note: The resizing operation ensures that the maximum dimension of the output matches the desired size, while preserving
the input aspect ratio. The compression level is tunable and varies between thumbnails and key images.

.. _IMOD: https://bio3d.colorado.edu/imod/
.. _dm2mrc: https://bio3d.colorado.edu/imod/doc/man/dm2mrc.html
.. _newstack: https://bio3d.colorado.edu/doc/man/newstack.html
.. _SimpleITK: https://simpleitk.readthedocs.io/
