import os
import tempfile
import unittest
from pathlib import Path

from wrongodb.blockfile import BlockFile, FileHeader
from wrongodb.errors import StorageError


class BlockFileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="wrongodb-tests-"))
        self.path = self.tmpdir / "test.wt"

    def tearDown(self) -> None:
        if self.path.exists():
            try:
                self.path.unlink()
            except OSError:
                pass
        try:
            os.rmdir(self.tmpdir)
        except OSError:
            # Directory might still contain files if a test failed early.
            for p in self.tmpdir.glob("*"):
                try:
                    p.unlink()
                except OSError:
                    pass
            try:
                os.rmdir(self.tmpdir)
            except OSError:
                pass

    def test_create_writes_header_and_one_block(self) -> None:
        bf = BlockFile.create(self.path, page_size=4096)
        self.assertEqual(bf.num_blocks(), 1)
        default_header = FileHeader()
        self.assertEqual(bf.header.magic, default_header.magic)
        self.assertEqual(bf.header.version, default_header.version)
        self.assertEqual(bf.header.page_size, 4096)
        bf.close()

        bf2 = BlockFile.open(self.path)
        self.assertEqual(bf2.page_size, 4096)
        self.assertEqual(bf2.num_blocks(), 1)
        bf2.close()

    def test_write_read_roundtrip(self) -> None:
        bf = BlockFile.create(self.path, page_size=1024)
        bf.write_block(1, b"hello")
        bf.write_block(2, b"world")
        self.assertEqual(bf.num_blocks(), 3)
        bf.close()

        bf2 = BlockFile.open(self.path)
        self.assertTrue(bf2.read_block(1).startswith(b"hello"))
        self.assertTrue(bf2.read_block(2).startswith(b"world"))
        bf2.close()

    def test_payload_too_large_raises(self) -> None:
        bf = BlockFile.create(self.path, page_size=128)
        max_payload = bf.page_size - 4
        with self.assertRaises(StorageError):
            bf.write_block(1, b"x" * (max_payload + 1))
        bf.close()

    def test_checksum_mismatch_detected(self) -> None:
        bf = BlockFile.create(self.path, page_size=512)
        bf.write_block(1, b"data")
        bf.close()

        # Corrupt a byte in the payload of block 1.
        with self.path.open("r+b") as fh:
            fh.seek(1 * 512 + 4)  # start of payload for block 1
            b = fh.read(1)
            fh.seek(1 * 512 + 4)
            fh.write(bytes([(b[0] ^ 0xFF) if b else 0xFF]))

        bf2 = BlockFile.open(self.path)
        with self.assertRaises(StorageError):
            bf2.read_block(1, verify=True)
        bf2.close()

    def test_create_on_existing_file_raises(self) -> None:
        bf = BlockFile.create(self.path)
        bf.close()
        with self.assertRaises(StorageError):
            BlockFile.create(self.path)


if __name__ == "__main__":
    unittest.main()
