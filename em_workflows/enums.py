from enum import Enum


class FileShareEnum(Enum):
    """
    Scientific data can reside in any mounted points depending on the lab or the project.
    This enum is used to direct where the data is mounted.
    Source: https://github.com/niaid/hedwig-comm-specs/issues/4#issuecomment-1701512859
    """

    RMLEMHedwigDev = 1
    RMLEMHedwigQA = 2
    RMLEMHedwigProd = 3
    # SO refers to spatial omics
    RMLSOHedwigDev = 4
    RMLSOHedwigQA = 5
    RMLSOHedwigProd = 6
