import json
from pathlib import Path
import tempfile
from image_portal_workflows.config import Config
import pytest


@pytest.fixture
def mock_fs(monkeypatch):
    monkeypatch.setattr(Config, "proj_dir", "/tmp/RMLEMHedwigQA/Projects")
    monkeypatch.setattr(Config, "assets_dir", "/tmp/RMLEMHedwigQA/Assets")
    monkeypatch.setattr(Config, "tmp_dir", "/tmp/")
    monkeypatch.setattr(Config, "mount_point", "/tmp/")


def test_make_work_dir_to_fp_copy_to_assets_dir(mock_fs):
    # create the place where outputs will go.
    # __wrapped__ jumps past decorator
    from image_portal_workflows.utils import utils

    project_dir_fp = Path("/tmp/RMLEMHedwigQA/Projects/Lab/PI/")
    assets_dir = utils.make_assets_dir.__wrapped__(input_dir=project_dir_fp)
    # check dir exists
    assert assets_dir.as_posix() == "/tmp/RMLEMHedwigQA/Assets/Lab/PI"
    # make temp workspace
    working_dir = utils.make_work_dir.__wrapped__(fname=Path("test_fn"))
    dummy_tg = Path(f"{project_dir_fp}/tg_name.mrc")
    dummy_asset = Path(f"{working_dir}/tg_name_ave.jpeg")
    dummy_asset.touch()
    asset_fp = utils.copy_to_assets_dir.__wrapped__(
        prim_fp=dummy_tg, fp=dummy_asset, assets_dir=assets_dir
    )
    assert (
        asset_fp.as_posix()
        == "/tmp/RMLEMHedwigQA/Assets/Lab/PI/tg_name/tg_name_ave.jpeg"
    )
    cb = utils.gen_callback_elt.__wrapped__(input_fname=dummy_tg)
    # print(dummy_tg.as_posix())
    # print(json.dumps(cb))
    # notice that the mount_point / project_dir_fp is stripped.
    assert (
        json.dumps(cb)
        == '{"primaryFilePath": "Lab/PI/tg_name.mrc", "title": "tg_name", "assets": []}'
    )

    jpeg_fp = utils.copy_to_assets_dir.__wrapped__(
        fp=dummy_asset, assets_dir=assets_dir, prim_fp=dummy_tg
    )
    cb_with_jpg = utils.add_assets_entry.__wrapped__(
        base_elt=cb, path=jpeg_fp, asset_type="thumbnail"
    )
    # print(json.dumps(cb_with_jpg))
    # notice the added asset.
    assert (
        json.dumps(cb_with_jpg)
        == '{"primaryFilePath": "Lab/PI/tg_name.mrc", "title": "tg_name", "assets": [{"type": "thumbnail", "path": "Lab/PI/tg_name/tg_name_ave.jpeg"}]}'
    )

    ng_dir = Path(f"{Config.assets_dir}/blah/dir_name")
    metadata = {"the": "metadata"}
    cb_with_ng = utils.add_assets_entry.__wrapped__(
        base_elt=cb_with_jpg,
        path=ng_dir,
        asset_type="neuroglancerPrecomputed",
        metadata=metadata,
    )
    print(json.dumps(cb_with_ng, indent=4))
    assert (
        json.dumps(cb_with_ng)
        == '{"primaryFilePath": "Lab/PI/tg_name.mrc", "title": "tg_name", "assets": [{"type": "thumbnail", "path": "Lab/PI/tg_name/tg_name_ave.jpeg"}, {"type": "neuroglancerPrecomputed", "path": "blah/dir_name", "metadata": {"the": "metadata"}}]}'
    )


def test_inputs_paired():
    from image_portal_workflows.brt.flow import check_inputs_paired

    fps = [Path("/tmp/fnamea.mrc"), Path("/tmp/fnameb.mrc"), Path("thing")]
    assert check_inputs_paired.__wrapped__(fps=fps) is True
    fps_no_pair = [
        Path("/tmp/fname.mrc"),
        Path("/tmp/fnameb.mrc"),
        Path("/tmp/fnameb.mrc"),
    ]
    assert check_inputs_paired.__wrapped__(fps=fps_no_pair) is False
    assert check_inputs_paired.__wrapped__(fps=[]) is False


def test_paired_files():
    from image_portal_workflows.brt.flow import list_paired_files

    fps = [Path("/tmp/fnamea.mrc"), Path("/tmp/fnameb.mrc"), Path("thing")]
    pairs = list_paired_files.__wrapped__(fnames=fps)
    assert len(pairs) == 1
    assert pairs[0].as_posix() == "/tmp/fname.mrc"
    fps_no_pairs = [
        Path("/tmp/fname.mrc"),
        Path("/tmp/fnameb.mrc"),
        Path("/tmp/fnameb.mrc"),
    ]
    assert list_paired_files.__wrapped__(fnames=fps_no_pairs) == []


def test_gen_mrc2tiff():
    from image_portal_workflows.brt import flow

    fake_mrc_fp = Path("/test/dir/fname.mrc")
    cmd = flow.gen_mrc2tiff.__wrapped__(fp=fake_mrc_fp)
    assert cmd == "mrc2tif -j -C 0,255 /test/dir/ali_fname.mrc /test/dir/fname_ali"


def test_copy_template_and_update_adoc():
    from image_portal_workflows.brt import flow

    with tempfile.TemporaryDirectory() as tmpdirname:
        wd = Path(tmpdirname)
        assert wd.exists
        fp = flow.copy_template.__wrapped__(working_dir=wd, template_name="plastic_brt")
        assert fp.exists
        fake_tg = Path("/fake/fake_tomogram.mrc")
        updated_adoc = flow.update_adoc.__wrapped__(
            adoc_fp=fp,
            tg_fp=fake_tg,
            dual_p=True,
            montage="1",
            gold="15",
            focus="0",
            bfocus="1",
            fiducialless="3",
            trackingMethod=None,
            TwoSurfaces="0",
            TargetNumberOfBeads="20",
            LocalAlignments="0",
            THICKNESS="30",
        )
        assert updated_adoc.exists
        with open(updated_adoc, "r") as _adoc:
            readfile = _adoc.read()
            print(readfile)  # show file
            assert "comparam.tilt.tilt.THICKNESS = 30" in readfile
            assert "setupset.copyarg.name = fake_tomogram" in readfile
            assert "trackingMethod" not in readfile
            assert "runtime.Positioning.any.thickness = 45" in readfile
            assert "setupset.copyarg.dual = 1" in readfile

        # flip dual_p to false
        updated_adoc = flow.update_adoc.__wrapped__(
            adoc_fp=fp,
            tg_fp=fake_tg,
            dual_p=False,
            montage="1",
            gold="15",
            focus="0",
            bfocus="1",
            fiducialless="3",
            trackingMethod=None,
            TwoSurfaces="0",
            TargetNumberOfBeads="20",
            LocalAlignments="0",
            THICKNESS="30",
        )
        with open(updated_adoc, "r") as _adoc:
            readfile = _adoc.read()
            assert "setupset.copyarg.dual = 0" in readfile
