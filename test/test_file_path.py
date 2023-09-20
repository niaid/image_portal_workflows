from em_workflows.file_path import FilePath


def test_gen_output_fp(mock_nfs_mount, tmp_path):
    input_dir = tmp_path / "Projects"
    input_dir.mkdir()
    p = input_dir / "some_file_rec.mrc"
    p.write_text("content")
    assert str(p).endswith("Projects/some_file_rec.mrc")

    fp_in = FilePath(share_name="test", input_dir=input_dir, fp_in=p)

    zarr = fp_in.gen_output_fp(output_ext=".zarr")
    rec_mrc = fp_in.gen_output_fp(output_ext="_rec.mrc")
    base_mrc = fp_in.gen_output_fp(output_ext=".mrc", out_fname="adjusted.mrc")
    input_mrc = fp_in.fp_in
    print(f"{rec_mrc=}\n{base_mrc=}\n{input_mrc=}")
    assert zarr.name == "some_file_rec.zarr"
    assert base_mrc.name == "adjusted.mrc"
    assert input_mrc.name == "some_file_rec.mrc"
    # Below is simply a temporary outfile loc
    assert rec_mrc.name == "some_file_rec_rec.mrc"
