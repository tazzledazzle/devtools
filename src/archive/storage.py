# added unified archive interface
import os
import zipfile
import tarfile
class Archive:
    def __init__(self, path):
        self.path = path
        self.archive_type = self._get_archive_type()

    def _get_archive_type(self):
        if zipfile.is_zipfile(self.path):
            return 'zip'
        elif tarfile.is_tarfile(self.path):
            return 'tar'
        else:
            raise ValueError(f"Unsupported archive type for file: {self.path}")

    def extract(self, extract_to):
        if self.archive_type == 'zip':
            with zipfile.ZipFile(self.path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
        elif self.archive_type == 'tar':
            with tarfile.open(self.path, 'r') as tar_ref:
                tar_ref.extractall(extract_to)

    def list_files(self):
        if self.archive_type == 'zip':
            with zipfile.ZipFile(self.path, 'r') as zip_ref:
                return zip_ref.namelist()
        elif self.archive_type == 'tar':
            with tarfile.open(self.path, 'r') as tar_ref:
                return tar_ref.getnames()
        return None


# ArchiveStorage protocol with write(event), list(), and replace()
# implementation follows archive-then-produce ordering

class ArchiveStorage:
    def write(self, event):
        """Write an event to the archive."""
        # write an event to the archive (append or create new)


    def list(self):
        """List all events in the archive."""
        raise NotImplementedError

    def replace(self, events):
        """Replace all events in the archive with the given list of events."""
        raise NotImplementedError