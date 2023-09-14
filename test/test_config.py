from prefect.engine import signals
import pytest

from em_workflows import config
from em_workflows.config import Config


class TestConfig:
    def test_mount_point_non_existent_path(self, tmp_path, monkeypatch):
        _mock_NFS_MOUNT = {"valid": str(tmp_path), "invalid": "/tmp/non-existent-path"}

        monkeypatch.setattr(config, "NFS_MOUNT", _mock_NFS_MOUNT)

        assert Config._mount_point("valid") == _mock_NFS_MOUNT["valid"]
        with pytest.raises(signals.FAIL) as excinfo:
            Config._mount_point("invalid")
        assert "invalid doesn't exist." in str(excinfo.value)
