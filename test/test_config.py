from prefect.engine import signals
import pytest

from em_workflows import config
from em_workflows.config import Config


class TestConfig:
    def test_mount_point_non_existent_path(self, tmp_path, monkeypatch):
        """
        Check mount point is retrieved properly
        """
        _mock_NFS_MOUNT = {"valid": str(tmp_path), "invalid": "/tmp/non-existent-path"}

        monkeypatch.setattr(config, "NFS_MOUNT", _mock_NFS_MOUNT)

        assert Config._mount_point("valid") == _mock_NFS_MOUNT["valid"]
        with pytest.raises(signals.FAIL) as excinfo:
            Config._mount_point("invalid")
        assert "invalid doesn't exist." in str(excinfo.value)

    def test_mount_point_bad_share_name(self, tmp_path, monkeypatch):
        """
        Check if the share name even exists
        """
        _mock_NFS_MOUNT = {"valid": str(tmp_path)}
        monkeypatch.setattr(config, "NFS_MOUNT", _mock_NFS_MOUNT)
        assert Config._mount_point("valid") == _mock_NFS_MOUNT["valid"]

        share_name = "Bad"
        with pytest.raises(signals.FAIL) as excinfo:
            Config._mount_point(share_name)
        assert f"{share_name} is not a valid name. Failing!" in str(excinfo)

        share_name = None
        with pytest.raises(signals.FAIL) as excinfo:
            Config._mount_point(share_name)
        assert f"{share_name} is not a valid name. Failing!" in str(excinfo)
