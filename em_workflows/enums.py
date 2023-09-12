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

    def get_mount_point(self):
        # TODO change the return values when decided
        mapper = {
            # each of the values will be different
            FileShareEnum.RMLSOHedwigDev: "/mnt/ai-fas12/RMLEMHedwigDev",
            FileShareEnum.RMLSOHedwigQA: "/mnt/ai-fas12/RMLEMHedwigQA",
            FileShareEnum.RMLSOHedwigProd: "/mnt/ai-fas12/RMLEMHedwigProd",
        }
        # default
        return mapper.get(self, f"/mnt/ai-fas12/{self.name}")
