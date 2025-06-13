******
Small 2D Workflow (EM Microscopy)
******

Overview:

This workflow generates a thumbnail and a key images for each input image in a directory. Specific EM file formats
use enhanced resampling and filtering techniques to reduce noise while adjusting the dynamic range of the images. This
workflow originally was designed to handle DM3 files from TEM or STEM microscopes but has been extended to handle
other 2D image file formats, including MRC, TIFF files and color figures.

Outputs:
 1. A thumbnail image for each input image.
   - An JPEG image with a maximum size of 300 pixels in both dimension.
   - The aspect ratio is preserved and may be shortened to fit within the maximum size without padding.
   - The image may be 8-bit RGB(A) or 8-bit gray-scale.
 2. A key image for each input image.
   - Similar to the thumbnail image but with a maximum size of 1024 pixels in both dimension.

.. list-table:: Supported Extensions for File Formats
   :header-rows: 1

   * - Format Type
     - Description
     - Extensions
   * - DM3
     - image format used by Digital Micrograph
     - dm3
   * - DM4
     - image format used by Digital Micrograph, maybe limited to 64-bit integers
     - dm4
   * - MRC
     - image format used by the IMOD tool suite
     - mrc
   * - TIFF
     - Standard Tag Image File Format ( TIFF 6.0 ), OME TIFF not supported
     - TIFF stacks not supported, only the first page is processed.
     - tif, tiff
   * - PNG
     - Portable Network Graphics
     - png
   * - JPEG
     - Joint Photographic Experts Group
     - jpg, jpeg


The toolbox of `IMOD`_ is used primarily to process the EM images, along with `SimpleITK`_ for additional image
processing tasks.


.. list-table:: Summary of Input Image Types
   :header-rows: 1

   * - Format Type
     - Pixel Type
     - Description
     - Processing Information
   * - JPEG, PNG, TIFF
     - 8-bit RGB (palette) or RGBA
     - Example images include figure, TEM grid image or other illustrative images rendered into a color image.
     - Images should directly resized without intensity scaling or advanced filtering.
   * - TIFF, PNG, JPEG
     - 8-bit gray-scale
     - Images may be rendered EM images with captions or annotations.
     - Images should directly resized without intensity scaling or advanced filtering.
   * - TIFF
     - 16-bit (signed or unsigned) gray-scale
     - EM images with a high dynamic range.
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

.. _IMOD: https://bio3d.colorado.edu/imod/
.. _dm2mrc: https://bio3d.colorado.edu/imod/doc/man/dm2mrc.html
.. _newstack: https://bio3d.colorado.edu/doc/man/newstack.html
.. _SimpleITK: https://simpleitk.readthedocs.io/
