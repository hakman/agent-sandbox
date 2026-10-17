# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape

from download_results import node_profile_files


def test_node_profile_manifest_rejects_paths_outside_artifacts():
    for name in ("../node-profile-secret", "/tmp/node-profile-secret", "node-profile-dir/file", "summary.json"):
        with pytest.raises(ValueError):
            list(node_profile_files([{"files": [name]}]))


def test_node_profile_links_include_partial_capture_status():
    records = [{"phase": "throughput-mif:100", "phaseNumber": 3, "node": "worker",
                "interrupted": True, "error": "capture <failed>",
                "files": ["node-profile-3-worker.jsonl", "pprof-containerd-3-worker.pprof"]}]
    assert list(node_profile_files(records)) == records[0]["files"]
    env = Environment(loader=FileSystemLoader(Path(__file__).parent / "templates"),
                      autoescape=select_autoescape(["html"]))
    rendered = env.get_template("pprof.html").render(
        node_profiles=json.loads(json.dumps(records)), pprof_profiles=[], summary={"cluster": {}}, active_page="pprof")
    assert "interrupted at phase end" in rendered
    assert "capture &lt;failed&gt;" in rendered
    for name in records[0]["files"]:
        assert f'href="{name}"' in rendered


def test_downloader_replaces_stale_node_profiles(tmp_path, monkeypatch):
    import sys
    import download_results

    stale = tmp_path / "node-profile-old.trace"
    stale.write_text("previous run")
    name = "node-profile-3-worker.trace"
    manifest = [{"files": [name]}]
    requested = []

    def download(url, output):
        requested.append(url)
        if output.name == "node-profiles.json":
            output.write_text(json.dumps(manifest))
        elif output.name == name:
            output.write_bytes(b"trace")
        else:
            return False
        return True

    monkeypatch.setattr(download_results, "download_file", download)
    monkeypatch.setattr(sys, "argv", ["download_results.py", "--url", "gs://bucket/run", "--output-dir", str(tmp_path)])
    download_results.main()
    assert not stale.exists()
    assert (tmp_path / name).read_bytes() == b"trace"
    assert f"https://storage.googleapis.com/bucket/run/{name}" in requested
