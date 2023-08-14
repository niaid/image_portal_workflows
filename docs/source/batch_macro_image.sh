#!/bin/env bash

# This script takes a list of czi files as arguments. For example:
#   batch_macro_image.sh /mnt/ai-fas12/RMLEMHedwigQA/TestData/Nanostringfiles/Collection\ X\ \(Most\ Recent\)/HQ_Images/*.czi
#
# It extracts the image in the czi file named "macro image" as tiff,
#   then convert and normized the range, and resample to 300x300 as a
#   jpeg.
#
# Bioformats (bftools) is require for this script.

echo "Inputs: $@"


IMAGE_NAME="macro image"
#Note: this could be changed to "label image"

_IMAGE_NAME="${IMAGE_NAME// /_}"

for fn in "$@";
do
    # The showinf command is used to extract the xml metadata from the
    # czi file. It is crudely parsed to extract the image series index
    # fot the image named "macro image".
    series_idx=$(~blowekamp/bftools/showinf -nopix -omexml-only "$fn" |grep "Name=\"${IMAGE_NAME}\"" | sed 's/[^0-9]*//g')
    bfn=$(basename "$fn")
    # Use Bioformats to extract the macro image and save as a TIFF
    # file. The output is a 16-bit RGB image ( the merge flag joings
    # the channels).
    ~blowekamp/bftools/bfconvert -series $series_idx  -merge "$fn" "${bfn%.*}_${_IMAGE_NAME}.tif"

    # The ImageMagik's convert command is use to generate a JPEG
    # thumbnail. The image is resized to fit within the standard
    # 300x300 thumbnail size, while preserving the aspect ratio. Also
    # the input 16-bit image just have information in the lowest ~10
    # bit, the intensities are rescaled automatically with the
    # normalize flag. Other flags were tried and this seem to give
    # best results out of the available options.
    convert  -size 300x300 "${bfn%.*}_${_IMAGE_NAME}.tif"  -normalize -resize 300x300 -quality 90 "${bfn%.*}_${_IMAGE_NAME}.jpg"
done
