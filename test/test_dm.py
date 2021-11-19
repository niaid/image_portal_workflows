import os
import sys

sys.path.append("..")

from image_portal_workflows.dm_conversion.flow import flow

def test_dm4_conv():
    result = flow.run(
        input_dir=os.getcwd()+"/test/input_files/",
    )
    assert result.is_successful()


