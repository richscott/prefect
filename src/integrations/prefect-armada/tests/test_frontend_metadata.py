"""Tests for the presentation metadata Armada supplies to the work pool UI."""

import base64
from importlib.resources import files
from pathlib import Path

from prefect_armada.worker import ArmadaWorker

from prefect.workers._logo import MAX_LOGO_BYTES, load_packaged_logo

DATA_URL_PREFIX = "data:image/svg+xml;base64,"


def packaged_logo_bytes() -> bytes:
    return (files("prefect_armada") / "frontend" / "armada.svg").read_bytes()


class TestArmadaLogo:
    def test_logo_is_served_from_the_installed_package(self):
        logo_url = ArmadaWorker.get_logo_url()

        assert logo_url.startswith(DATA_URL_PREFIX)
        assert (
            base64.b64decode(logo_url[len(DATA_URL_PREFIX) :]) == packaged_logo_bytes()
        )

    def test_packaged_logo_takes_precedence_over_the_external_url(self):
        assert ArmadaWorker._logo_resource == "frontend/armada.svg"
        assert ArmadaWorker._logo_url.startswith("https://")
        assert ArmadaWorker.get_logo_url() != ArmadaWorker._logo_url

    def test_logo_fits_within_the_size_cap(self):
        assert 0 < len(packaged_logo_bytes()) <= MAX_LOGO_BYTES

    def test_logo_is_a_standalone_image(self):
        """An `<img>` element must be able to render it without fetching anything."""
        markup = packaged_logo_bytes().decode("utf-8").lower()

        assert markup.lstrip().startswith(("<?xml", "<svg"))
        for forbidden in ("<script", "foreignobject", 'href="http', "url(http"):
            assert forbidden not in markup

    def test_packaged_asset_matches_the_source_tree(self):
        source = (
            Path(__file__).parents[1] / "prefect_armada" / "frontend" / "armada.svg"
        )

        assert source.read_bytes() == packaged_logo_bytes()

    def test_attribution_ships_beside_the_logo(self):
        attribution = files("prefect_armada") / "frontend" / "ATTRIBUTION.md"

        assert "Apache License 2.0" in attribution.read_text()

    def test_logo_failure_would_fall_back_to_the_external_url(self, monkeypatch):
        """The external URL stays meaningful if the asset ever goes missing."""
        monkeypatch.setattr(ArmadaWorker, "_logo_resource", "frontend/absent.svg")
        load_packaged_logo.cache_clear()

        assert ArmadaWorker.get_logo_url() == ArmadaWorker._logo_url

        load_packaged_logo.cache_clear()


class TestArmadaPresentation:
    def test_presentation_strings_stay_on_the_worker(self):
        assert ArmadaWorker.get_display_name() == "Armada"
        assert "Armada cluster" in ArmadaWorker.get_description()
        assert ArmadaWorker.get_documentation_url().startswith("https://")

    def test_configuration_schema_still_comes_from_the_worker_models(self):
        template = ArmadaWorker.get_default_base_job_template()

        assert set(template) == {"job_configuration", "variables"}
        assert "armada_host" in template["variables"]["properties"]
