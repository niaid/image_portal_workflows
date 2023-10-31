*********
Spatialomics Workflow
*********

Normally the dir structure is : ``$lab/$pi/$project/$session/$sample``

For Spatialomics this is not the case, the $sample is not really a sample, it's grouping of ROIs, PreROIs, etc from each of the slides.
These grouping directories sit one down from session, in the place a ``$sample`` normally would be.


For a single unit of work (eg 8 slides) the set up looks like

.. code-block::

   $lab/$pi/$project/$session/Pre_ROI_Selection
   $lab/$pi/$project/$session/Heatmaps
   $lab/$pi/$project/$session/HQ_Images
   $lab/$pi/$project/$session/ROI_Images


and eg:

.. code-block::

   ls $lab/$pi/$project/$session/HQ_Images/
   slide_1.czi, slide_2.czi, ...

and

.. code-block::

   ls $lab/$pi/$project/$session/Pre_ROI_Selection
   slide_1_Pre_ROI_a.png, slide_2_Pre_ROI_a.png, ...


Note:
Different outputs from different slides are split across different dirs.
The pipeline will not parse out or otherwise link slides together in any way.
Similarly there is no linking from from heatmaps to any specific slide.

Also, other directories can exist in this directory, which will be ignored.
