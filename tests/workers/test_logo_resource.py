"""Tests for logos that an integration ships inside its own Python package."""

import base64
import importlib
import io
import itertools
import sys

import pytest

from prefect.workers import _logo
from prefect.workers._logo import MAX_LOGO_BYTES, load_packaged_logo
from prefect.workers.base import BaseJobConfiguration, BaseWorker

SVG_BYTES = (
    b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 8 8">'
    b'<rect width="8" height="8" fill="#123456"/></svg>'
)
PNG_BYTES = base64.b64decode(
    b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8"
    b"AAAAASUVORK5CYII="
)
LEGACY_URL = "https://example.com/legacy-logo.png"

# Worker classes register themselves by `type`, so each test builds a distinct one
# rather than overriding an entry another test left in the registry.
_worker_types = itertools.count()


def decoded_payload(data_url: str) -> bytes:
    return base64.b64decode(data_url.split(",", 1)[1])


@pytest.fixture
def logo_package(tmp_path, monkeypatch):
    """Builds and imports a throwaway package that ships assets like an integration."""
    package_name = "prefect_logo_fixture"
    package_dir = tmp_path / package_name
    (package_dir / "frontend" / "nested").mkdir(parents=True)
    (package_dir / "frontend" / "directory.svg").mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "frontend" / "logo.svg").write_bytes(SVG_BYTES)
    (package_dir / "frontend" / "logo.png").write_bytes(PNG_BYTES)
    (package_dir / "frontend" / "empty.svg").write_bytes(b"")
    (package_dir / "frontend" / "notes.txt").write_text("not an image")
    (package_dir / "frontend" / "exactly-at-cap.svg").write_bytes(b"a" * MAX_LOGO_BYTES)
    (package_dir / "frontend" / "over-cap.svg").write_bytes(b"a" * (MAX_LOGO_BYTES + 1))
    # Sits outside frontend/ so a traversal attempt has something to reach for.
    (package_dir / "outside.svg").write_bytes(b"<svg/>")

    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    load_packaged_logo.cache_clear()

    yield package_name

    for module in [
        name
        for name in sys.modules
        if name == package_name or name.startswith(f"{package_name}.")
    ]:
        del sys.modules[module]
    load_packaged_logo.cache_clear()


class TestLoadPackagedLogo:
    @pytest.mark.parametrize(
        "resource",
        ["C:/outside.svg", "C:outside.svg", "frontend/C:/outside.svg"],
    )
    def test_drive_qualified_paths_are_rejected_before_resource_lookup(
        self, monkeypatch, resource
    ):
        def fail(package):
            raise AssertionError("drive-qualified paths must not reach package lookup")

        monkeypatch.setattr(_logo, "files", fail)
        load_packaged_logo.cache_clear()

        assert load_packaged_logo("any-package", resource) is None

    def test_svg_becomes_a_data_url_carrying_the_exact_bytes(self, logo_package):
        data_url = load_packaged_logo(logo_package, "frontend/logo.svg")

        assert data_url is not None
        assert data_url.startswith("data:image/svg+xml;base64,")
        assert decoded_payload(data_url) == SVG_BYTES

    def test_png_becomes_a_data_url_carrying_the_exact_bytes(self, logo_package):
        data_url = load_packaged_logo(logo_package, "frontend/logo.png")

        assert data_url is not None
        assert data_url.startswith("data:image/png;base64,")
        assert decoded_payload(data_url) == PNG_BYTES

    def test_resource_exactly_at_the_cap_is_accepted(self, logo_package):
        data_url = load_packaged_logo(logo_package, "frontend/exactly-at-cap.svg")

        assert data_url is not None
        assert len(decoded_payload(data_url)) == MAX_LOGO_BYTES

    def test_resource_one_byte_over_the_cap_is_rejected(self, logo_package):
        assert load_packaged_logo(logo_package, "frontend/over-cap.svg") is None

    def test_reads_at_most_one_byte_past_the_cap(self, monkeypatch):
        """An oversized asset must never be read into memory in full."""
        requested_sizes: list[int] = []

        class RecordingHandle(io.BytesIO):
            def read(self, size: int = -1) -> bytes:
                requested_sizes.append(size)
                return super().read(size)

        class StubTraversable:
            def joinpath(self, *parts: str) -> "StubTraversable":
                return self

            def open(self, mode: str = "r") -> RecordingHandle:
                return RecordingHandle(b"a" * 32)

        monkeypatch.setattr(_logo, "files", lambda package: StubTraversable())
        load_packaged_logo.cache_clear()

        assert load_packaged_logo("any-package", "frontend/logo.svg") is not None
        assert requested_sizes == [MAX_LOGO_BYTES + 1]

        load_packaged_logo.cache_clear()

    @pytest.mark.parametrize(
        "resource,reason",
        [
            ("frontend/missing.svg", "missing file"),
            ("frontend/empty.svg", "empty file"),
            ("frontend/directory.svg", "directory named like an image"),
            ("frontend/nested", "directory"),
            ("frontend", "directory"),
            ("frontend/notes.txt", "unsupported suffix"),
            ("frontend/logo.SVG", "uppercase suffix"),
            ("frontend/.svg", "suffix with no name"),
            ("../outside.svg", "parent traversal"),
            ("frontend/../../outside.svg", "traversal through a valid prefix"),
            ("./frontend/logo.svg", "current-directory component"),
            ("frontend//logo.svg", "empty component"),
            ("/etc/hosts.svg", "absolute path"),
            ("frontend\\logo.svg", "backslash separator"),
            ("frontend/logo.svg\x00.txt", "NUL byte"),
            ("", "empty resource"),
        ],
    )
    def test_unusable_resources_return_none(self, logo_package, resource, reason):
        assert load_packaged_logo(logo_package, resource) is None, reason

    def test_missing_package_returns_none(self):
        load_packaged_logo.cache_clear()

        assert (
            load_packaged_logo("no_such_package_anywhere", "frontend/logo.svg") is None
        )

        load_packaged_logo.cache_clear()

    def test_results_are_cached_including_failures(self, logo_package):
        load_packaged_logo.cache_clear()

        load_packaged_logo(logo_package, "frontend/logo.svg")
        load_packaged_logo(logo_package, "frontend/logo.svg")
        load_packaged_logo(logo_package, "frontend/missing.svg")
        load_packaged_logo(logo_package, "frontend/missing.svg")

        cache_info = load_packaged_logo.cache_info()
        assert cache_info.hits == 2
        assert cache_info.misses == 2


class TestWorkerLogoResource:
    """`BaseWorker.get_logo_url` behavior when a worker ships its own logo."""

    def build_worker(self, logo_package, **attributes):
        class PackagedLogoWorker(BaseWorker):
            type = f"test-packaged-logo-{next(_worker_types)}"
            job_configuration = BaseJobConfiguration

            _description = "A worker that ships its own logo."
            _display_name = "Packaged Logo"
            _documentation_url = "https://example.com/docs"
            _logo_url = LEGACY_URL

            async def run(self):
                pass

            async def verify_submitted_deployment(self, deployment):
                pass

        for name, value in attributes.items():
            setattr(PackagedLogoWorker, name, value)
        # Resources resolve against the worker's own top-level package.
        PackagedLogoWorker.__module__ = f"{logo_package}.worker"
        return PackagedLogoWorker

    def test_packaged_logo_wins_over_the_legacy_url(self, logo_package):
        worker = self.build_worker(logo_package, _logo_resource="frontend/logo.svg")

        assert decoded_payload(worker.get_logo_url()) == SVG_BYTES

    def test_unreadable_resource_falls_back_to_the_legacy_url(self, logo_package):
        worker = self.build_worker(logo_package, _logo_resource="frontend/missing.svg")

        assert worker.get_logo_url() == LEGACY_URL

    def test_unreadable_resource_leaves_other_metadata_intact(self, logo_package):
        worker = self.build_worker(logo_package, _logo_resource="frontend/missing.svg")

        assert worker.get_display_name() == "Packaged Logo"
        assert worker.get_description() == "A worker that ships its own logo."
        assert worker.get_documentation_url() == "https://example.com/docs"
        assert "variables" in worker.get_default_base_job_template()

    def test_packaged_logo_leaves_the_template_unchanged(self, logo_package):
        packaged = self.build_worker(logo_package, _logo_resource="frontend/logo.svg")
        legacy = self.build_worker(logo_package)

        assert (
            packaged.get_default_base_job_template()
            == legacy.get_default_base_job_template()
        )

    def test_worker_without_a_resource_keeps_its_exact_legacy_url(self, logo_package):
        worker = self.build_worker(logo_package)

        assert worker.get_logo_url() == LEGACY_URL

    def test_worker_without_a_resource_never_reads_a_resource(
        self, logo_package, monkeypatch
    ):
        def fail(*args, **kwargs):
            raise AssertionError("the resource reader must not be consulted")

        monkeypatch.setattr("prefect.workers.base.load_packaged_logo", fail)

        assert self.build_worker(logo_package).get_logo_url() == LEGACY_URL
