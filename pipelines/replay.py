"""Local single-host batch locks and immutable published-output verification."""
import fcntl
import hashlib
from contextlib import contextmanager
from pathlib import Path


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            hasher.update(chunk)
    return hasher.hexdigest()


@contextmanager
def batch_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('Another process is running this batch; retry after it finishes') from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
    # Keep the lock inode: deleting it can allow two processes to lock different files.


def output_digests(outputs: dict) -> dict:
    result = {}
    for location in outputs.values():
        table = Path(location)
        if not (table / '_delta_log' / '00000000000000000000.json').is_file():
            raise ValueError(f'Missing committed output: {table}')
        for path in sorted(table.rglob('*')):
            if path.is_file() and path.suffix in ('.parquet', '.json'):
                result[str(path)] = digest(path)
    return result


def verify_manifest(manifest: dict, source_digest: str, policy: str) -> None:
    if manifest['source_sha256'] != source_digest or manifest['policy_version'] != policy:
        raise ValueError('Batch already completed with different source content or policy; use a new batch ID/root')
    if output_digests(manifest['outputs']) != manifest['artifacts']:
        raise ValueError('Published output changed or is incomplete; preserve it and use a new root to rebuild')
