************
BRT Workflow
************

Overview:

A number of parameters can be passed to the "BRT" workflow. There are two types:
1) params that are passed to the adoc file. See adoc / https://bio3d.colorado.edu/imod/doc/directives.html
2) params that are used elsewhere within the worflow, providing metadata etc, eg input_dir.


Note: objects passed between Prefect Tasks (eg FilePath objects), must be considered immutable. Updates to state made in one task will be lost and not available to the next task. Create a new object.
The map function is used extensively, and preserves order.

TomoJS 3D pipeline for tomographic reconstruction.

The term "batch run tomo[gram]" is abbreviated BRT.


Processes
---------

Generate BRT Directive File
+++++++++++++++++++++++++++

Description:
 Generate a configuration file of parameter for BRT process.

Outputs:
 * a BRT directive file with a `.adoc` file extension.

Alternatively, the file could be provided and this step skipped. The directive file format is documented IMOD's
`batchruntomo`_ documentation. There is specific descriptions of the `directives`_ specified in a "directives.csv"
file within IMOD.

The following is a Jinja like template for key-value pairs to be configured. The `{{ }}` denotes where the substitution
should be made. Inside the curly brackets is a suggested parameter name for the value with defaults in parenthesis:

::

   setupset.copyarg.name={{ basename }}
   setupset.copyarg.gold={{ beadSize (~10)}}
   setupset.copyarg.pixel={{ pixelSize }}
   comparam.prenewst.newstack.BinByFactor={{ bin (4) }}
   comparam.track.beadtrack.LightBeads={{ lightBeads (0) }}
   comparam.tilt.tilt.THICKNESS={{ thickness (~256) }}
   runtime.AlignedStack.any.binByFactor={{ bin }}
   setupset.copyarg.montage={{ montage (0) }}
   setupset.datasetDirectory={{ local processing folder (POSIX convention) }}


The existing directive file is `dirTemplate.adoc` with empty values that can be updated as above.

BRT support multiple layers of "Template Files"  so that directives can be defined for a microscope, system, and user.
The other directive files can be defined as parameters it the batch directive file provided on the command line.


BatchRunTomo (BRT)
++++++++++++++++++

Description:
 Perform computational expensive operations of processing an acquired tomography tilt-series and reconstructing a 3D
 volume. The process consists of numerous step to prepare, align, and perform 3d reconstruction from the tilt series
 acquired by the microscope.

Inputs:
 1. the original image from the microscope usually with an `.mrc` extension (`.st` possible ). The filename part before
    the final "." is considered the `BASENAME`.
 2. BRT directive file

Primary Outputs:
 1. `BASENAME_rec.mrc` - the source for the reconstruction movie
 2. `BASENAME_full_rec.mrc` - the source for the Neuroglancer pyramid
 3. `BASENAME_ali.mrc` - the source for the tilt movie

Auxiliary Outputs:
 1. Other outputs such as logs and transformation files may need to be saved as well.


The process is IMOD's `batchruntomo`_, run with the following command line arguments:

::

    batchruntomo -di BASENAME.adoc -gpus 1 -cpus 20

The `gpus` is for configuring the local GPU, and `cpus` configures the number of cores to use.  Many
temporary and log files are created during this process.


Generate tilt movie
+++++++++++++++++++

Description:
  Convert a MRC file of the aligned tilt series into a movie for easy viewing.

Inputs:
 1. `BASENAME_ali.mrc` - the aligned 2d slices of image stack

Outputs:
 1. Generates a tilt movie for easy viewing
 2. The key thumbnail is  keyimg_BASENAME_s.jpg, the corresponding MIDDLE_I is the full size.


::

    dimensions = (header -s ${BASENAME}_ali.mrv) # space separated list of dimensions sizes (x y z)
    MIDDLE_I = floor(dimensions.z/2))


::

    for i in dimensions.z\:
      newstack -secs {i}-{i} ALI_FILENAME WORKDIR/hedwig/BASENAME_ali{i}.mrc
    newstack -float 3 WORKDIR/hedwig/BASENAME_ali*.mrc WORKDIR/hedwig/ali_BASENAME.mrc
    mrc2tif -j -C 0,255 WORKDIR/hedwig/ali_BASENAME.mrc WORKDIR/hedwig/BASENAME_ali
    gm convert -size 300x300 WORKDIR/hedwig/BASENAME_ali.{MIDDLE_I}.jpg -resize 300x300 -sharpen 2 -quality 70 WORKDIR/hedwig/keyimg_BASENAME_s.jpg
    ffmpeg -f image2 -framerate 4 -i ${BASENAME}_ali.%03d.jpg -vcodec libx264 -pix_fmt yuv420p -s 1024,1024 tiltMov_${BASENAME}.mp4


Generate reconstruction movie
+++++++++++++++++++++++++++++

Description:
  Convert a MRC file of the reconstructed 3D volume into a movie for easy viewing.

Inputs:
 1. `BASENAME_rec.mrc` - the reconstruction of the 3d volume ( may already be binned by some factor when compared to full).

Outputs:
 2. Generates a movie of the reconstructed 3D volume.

::

    for i in range(2, dimensions.z-2):
      IZMIN = i-2
      IZMAX = i+2
      clip avg -2d -iz IZMIN-IZMAX  -m 1 WORKDIR/BASENAME_rec.mrc WORKDIR/hedwig/BASENAME_ave${I}.mrc
    newstack -float 3 WORKDIR/hedwig/BASENAME_ave* WORKDIR/hedwig/ave_BASENAME.mrc
    binvol -binning 2 WORKDIR/hedwig/ave_BASENAME.mrc WORKDIR/hedwig/avebin8_BASENAME.mrc
    mrc2tif -j -C 100,255 WORKDIR/hedwig/ave_BASNAME.mrc hedwig/BASENAME_mp4
    ffmpeg -f image2 -framerate 8 -i WORKDIR/hedwig/BASENAME_mp4.%04d.jpg -vcodec libx264 -pix_fmt yuv420p -s 1024,1024 WORKDIR/hedwig/keyMov_BASENAME.mp4


Generate Neuroglancer Pyramid
+++++++++++++++++++++++++++++

Descriptions:
  Generates a `Neuroglancer`_ `precomputed`_ pyramid from an MRC file of a 3D volume. This does not work for tilt series, or other
  non-volumetric files.

Inputs:
 1. A MRC file of a 3D volume.

Outputs:
 1. A directory structure of the `precomputed`_ pyramid.

Steps:
  1. Convert the MRC file to NIFTI (`.nii`).
  The Neuroglancer file format only support unsigned integer of 8 or 16 bits. When this input is a signed integer the
  output pixel types needs to be changed and the pixel values adjusted. The NIAID `tomojs-pytools`_ `mrc2nift`
  command-line tool can do this conversion.
  2. The `neuroglancer-scripts`_ tools are used to convert the NIFTI file to the precompute format:
  ::

    volume-to-precomputed-pyramid --downscaling-method=average --no-gzip --flat nifti.nii {WORKDIR}/hedwig/neuro-BASENAME

  3. The default minimum and maximum values used for visualization also need to be computed from the NIFTI file. The
  NIAID `tomojs-pytools`_ `mrc_visual_min_max` performs this computation:
  ::

    mrc_visual_min_max {WORKDIR}/nifti.nii --mad 5 --output-json mrc2ngpc-output.json


The `cloud-volume`_ tool may be an alternative tool for the precompute conversion.

.. _batchruntomo: https://bio3d.colorado.edu/imod/doc/man/batchruntomo.html
.. _directives: https://bio3d.colorado.edu/imod/doc/directives.html
.. _neuroglancer-scripts: https://github.com/HumanBrainProject/neuroglancer-scripts
.. _precomputed: https://github.com/google/neuroglancer/blob/master/src/neuroglancer/datasource/precomputed/volume.md
.. _neuroglancer: https://github.com/google/neuroglancer
.. _tomojs-pytools: https://github.com/niaid/tomojs-pytools
.. _cloud-volume: https://github.com/seung-lab/cloud-volume
