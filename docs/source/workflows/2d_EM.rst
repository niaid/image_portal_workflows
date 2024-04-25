****
2D Electron Microscopy Workflow ( formally known as DM conversion workflow)
****

Overview:

This workflow originally was designed to handle DM3 files form TEM or STEM microscopes but has been extended to handle
other 2D EM image file formats.

Outputs:
 1. A thumbnail image for each the input images
 2. A key image for each the input images.

The tools box of `IMOD`_ is used primarily to process the EM images.

Format Conversion
+++++++++++++++++

The first step is to convert the input to a format compatible with the IMOD tool suit (e.g. mrc or tiff).

Inputs: DM3 or DM4 files
Outputs: MRC files
Steps:
    1. Use the `dm2mrc`_ tool to convert the input files to MRC format.

Resampling
++++++++++

The EM images can be broadly characterized has have a low signal to noise ratio and a high dynamic range with potential
outliers. The resampling or resizing of these images requires advanced algorithms and options that are not available
with conventional image processing tools. Through proper resampling and filtering the images signal to noise ratio can
be improved and the dynamic range can be reduced for better visualization.

The IMOD tool `newstack`_ is used to resample the images.

Inputs: MRC or TIFF files
Outputs: TIFF files as 8-bit image
Steps:
  1. Determine the size or dimensions of the input images.
  2. Compute the factor to reduce the size of the input image to the desired key image size.
  3. Use the `newstack`_ tool to filter and resize/shrink the input image to approximately the desired size.

.. code-block:: bash
 newstack -format TIFF -shrink $shrink_factor -antialias 6 -mode 0 -meansd "140,50" input.tiff output.tiff

The above command should convert any supported scalar pixel input type to an 8-bit image which a dynamic range
reasonable for visualization. The `meansd` values could be considered tunable parameters.

Output Generation
+++++++++++++++++

The final step is to generate the thumbnail and key images with GraphicsMagick. The compression and filtering options
are used for web display.

Inputs: TIFF files
Outputs: JPEG files
Steps:
  1. Use GraphicsMagick to generate the thumbnail and key images.

.. code-block:: bash
 gm convert -size $output_size -resize $output_size -sharpen 2 -quality 80 input.tiff thumbnail.jpg

Note: The "resize" option specified the maximum size of a dimension of the output, the other dimension man be smaller to
preserve the input aspect ration.

The sharpen and quality value are tunable parameters which may vary between thumbnails and key images or may need to be
further turned based on the input image.

.. _IMOD: https://bio3d.colorado.edu/imod/
.. _dm2mrc: https://bio3d.colorado.edu/imod/doc/man/dm2mrc.html
.. _newstack: https://bio3d.colorado.edu/doc/man/newstack.html
