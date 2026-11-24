"""Local locking and atomic report behavior without a JVM."""
import json
import pytest
from pipelines.replay import batch_lock
from pipelines.run import save_json


def test_lock_excludes_second_holder_and_releases_on_error(tmp_path):
    path = tmp_path / 'batch.lock'
    with pytest.raises(RuntimeError):
        with batch_lock(path):
            with pytest.raises(ValueError, match='Another process'):
                with batch_lock(path):
                    pytest.fail('Lock was not exclusive')
            raise RuntimeError('crash')
    with batch_lock(path):
        assert path.exists()


def test_failed_json_serialization_preserves_existing_report(tmp_path):
    path = tmp_path / 'report.json'
    save_json(path, {'status': 'running'})
    with pytest.raises(ValueError):
        save_json(path, {'value': float('nan')})
    assert json.loads(path.read_text()) == {'status': 'running'}
    assert list(tmp_path.iterdir()) == [path]


def test_execution_reservation_cannot_overwrite_report(tmp_path):
    path = tmp_path / "execution.json"
    save_json(path, {"status": "running"}, exclusive=True)
    with pytest.raises(FileExistsError):
        save_json(path, {"status": "different"}, exclusive=True)
    assert json.loads(path.read_text()) == {"status": "running"}
    assert list(tmp_path.iterdir()) == [path]
