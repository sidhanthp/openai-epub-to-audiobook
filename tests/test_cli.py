from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
import zipfile
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from epub_to_audiobook.cli import (
    BATCH_ENDPOINT,
    escape_ffmetadata,
    local_cleanup,
    make_batch_requests,
    materialize_batch_outputs,
    merge_all,
    prepare_outputs,
    resolve_media_tool,
    resolve_runtime_settings,
    resolve_epub_paths,
    resolve_tts_backend,
    write_chapter_metadata_file,
)


def build_test_epub(path: Path) -> None:
    container_xml = """\
    <?xml version="1.0" encoding="UTF-8"?>
    <container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
      <rootfiles>
        <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
      </rootfiles>
    </container>
    """
    content_opf = """\
    <?xml version="1.0" encoding="utf-8"?>
    <package xmlns="http://www.idpf.org/2007/opf" unique-identifier="BookId" version="2.0">
      <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
        <dc:title>Test Book</dc:title>
        <dc:creator>Test Author</dc:creator>
        <dc:language>en</dc:language>
        <dc:identifier id="BookId">book-id</dc:identifier>
      </metadata>
      <manifest>
        <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
        <item id="chap1" href="chapter1.xhtml" media-type="application/xhtml+xml"/>
        <item id="chap2" href="chapter2.xhtml" media-type="application/xhtml+xml"/>
      </manifest>
      <spine toc="ncx">
        <itemref idref="chap1"/>
        <itemref idref="chap2"/>
      </spine>
    </package>
    """
    toc_ncx = """\
    <?xml version="1.0" encoding="UTF-8"?>
    <ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
      <navMap>
        <navPoint id="navPoint-1" playOrder="1">
          <navLabel><text>Chapter One</text></navLabel>
          <content src="chapter1.xhtml"/>
        </navPoint>
        <navPoint id="navPoint-2" playOrder="2">
          <navLabel><text>Chapter Two</text></navLabel>
          <content src="chapter2.xhtml"/>
        </navPoint>
      </navMap>
    </ncx>
    """
    chapter1 = """\
    <html xmlns="http://www.w3.org/1999/xhtml"><body>
      <h1>Chapter One</h1>
      <p>First paragraph.</p>
      <p>Second paragraph with Test Book in the middle.</p>
    </body></html>
    """
    chapter2 = """\
    <html xmlns="http://www.w3.org/1999/xhtml"><body>
      <h1>Chapter Two</h1>
      <p>Third paragraph.</p>
    </body></html>
    """
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("META-INF/container.xml", textwrap.dedent(container_xml))
        zf.writestr("OEBPS/content.opf", textwrap.dedent(content_opf))
        zf.writestr("OEBPS/toc.ncx", textwrap.dedent(toc_ncx))
        zf.writestr("OEBPS/chapter1.xhtml", textwrap.dedent(chapter1))
        zf.writestr("OEBPS/chapter2.xhtml", textwrap.dedent(chapter2))


class CliTests(unittest.TestCase):
    def test_resolve_epub_paths_uses_container_xml(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            epub_path = Path(tmpdir) / "book.epub"
            build_test_epub(epub_path)
            with zipfile.ZipFile(epub_path) as zf:
                self.assertEqual(resolve_epub_paths(zf), ("OEBPS/content.opf", "OEBPS"))

    def test_local_cleanup_keeps_title_mentions_in_body(self) -> None:
        cleaned = local_cleanup(
            "Test Book",
            "Chapter One",
            "Test Book\n\nChapter One\n\nA paragraph that mentions Test Book in the body.",
        )
        self.assertIn("mentions Test Book in the body", cleaned)
        self.assertTrue(cleaned.startswith("Chapter One\n\n"))

    def test_prepare_outputs_isolates_full_and_selection_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            epub_path = tmp / "book.epub"
            build_test_epub(epub_path)
            common = dict(
                epub_path=epub_path,
                out_root=tmp / "output",
                voice="marin",
                speed=1.5,
                audio_format="mp3",
                final_format="m4b",
                tts_model="gpt-4o-mini-tts",
                clean_model="gpt-5.4-mini",
                instructions="Test instructions",
                max_chars=3200,
                cleanup_max_chars=12000,
                skip_titles=set(),
                use_clean_model=False,
                clean_backend="local",
                cleanup_workers=1,
            )
            full = prepare_outputs(
                **common,
                chapter_numbers=[],
                chapter_matches=[],
            )
            sample = prepare_outputs(
                **common,
                chapter_numbers=[1],
                chapter_matches=[],
            )
            self.assertNotEqual(full["base_dir"], sample["base_dir"])
            self.assertIn("full-book", str(full["base_dir"]))
            self.assertIn("chapter-one", str(sample["base_dir"]))
            self.assertEqual(Path(full["final_book_path"]).suffix, ".m4b")

    def test_list_chapters_does_not_require_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            epub_path = tmp / "book.epub"
            build_test_epub(epub_path)
            env = os.environ.copy()
            env["PYTHONPATH"] = "src"
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "epub_to_audiobook.cli",
                    str(epub_path),
                    "--list-chapters",
                    "--secret-file",
                    str(tmp / "missing.env"),
                ],
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("Chapter One", proc.stdout)
            self.assertIn("Chapter Two", proc.stdout)

    def test_tts_backend_defaults_to_batch_and_rush_switches_to_live(self) -> None:
        self.assertEqual(resolve_tts_backend(None, False), "batch")
        self.assertEqual(resolve_tts_backend(None, True), "live")
        self.assertEqual(resolve_tts_backend("live", False), "live")
        with self.assertRaises(SystemExit):
            resolve_tts_backend("batch", True)

    def test_render_settings_prefer_saved_metadata_until_overridden(self) -> None:
        args = Namespace(
            audio_format=None,
            final_format=None,
            speed=None,
            voice=None,
            tts_model=None,
            instructions=None,
        )
        audio_format, final_format, speed, voice, tts_model, instructions = resolve_runtime_settings(
            args,
            render_metadata={
                "audio_format": "flac",
                "final_format": "m4b",
                "speed": 1.5,
                "voice": "cedar",
                "tts_model": "gpt-4o-mini-tts",
            },
        )
        self.assertEqual(audio_format, "flac")
        self.assertEqual(final_format, "m4b")
        self.assertEqual(speed, 1.5)
        self.assertEqual(voice, "cedar")
        self.assertEqual(tts_model, "gpt-4o-mini-tts")
        self.assertIn("1.5x output speed", instructions)

        override_args = Namespace(
            audio_format="mp3",
            final_format="m4b",
            speed=1.0,
            voice="marin",
            tts_model="override-model",
            instructions="Custom instructions",
        )
        override = resolve_runtime_settings(
            override_args,
            render_metadata={
                "audio_format": "flac",
                "speed": 1.5,
                "voice": "cedar",
                "tts_model": "gpt-4o-mini-tts",
            },
        )
        self.assertEqual(override, ("mp3", "m4b", 1.0, "marin", "override-model", "Custom instructions"))

    def test_wait_rejects_rush_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            epub_path = tmp / "book.epub"
            build_test_epub(epub_path)
            env = os.environ.copy()
            env["PYTHONPATH"] = "src"
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "epub_to_audiobook.cli",
                    str(epub_path),
                    "--wait",
                    "--rush",
                ],
                cwd=Path(__file__).resolve().parents[1],
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("--wait can only be used with batch TTS", proc.stderr)

    def test_batch_request_generation_and_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            requests_path = tmp / "requests.jsonl"
            map_path = tmp / "map.json"
            jobs = [
                {
                    "input": "Hello world",
                    "out": "01-chapter/001.mp3",
                }
            ]
            total, request_map = make_batch_requests(
                pending_jobs=jobs,
                tts_model="gpt-4o-mini-tts",
                voice="marin",
                audio_format="mp3",
                instructions="Read clearly",
                requests_path=requests_path,
                map_path=map_path,
            )
            self.assertEqual(total, 1)
            line = json.loads(requests_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(line["url"], BATCH_ENDPOINT)
            self.assertEqual(line["body"]["audio"]["voice"], "marin")
            self.assertEqual(line["body"]["audio"]["format"], "mp3")
            self.assertEqual(request_map["tts-00001"], "01-chapter/001.mp3")

            output_path = tmp / "output.jsonl"
            audio_bytes = b"fake-mp3"
            output_path.write_text(
                json.dumps(
                    {
                        "custom_id": "tts-00001",
                        "response": {
                            "status_code": 200,
                            "body": {
                                "choices": [
                                    {
                                        "message": {
                                            "audio": {
                                                "data": base64.b64encode(audio_bytes).decode("utf-8")
                                            }
                                        }
                                    }
                                ]
                            },
                        },
                        "error": None,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            completed, failed = materialize_batch_outputs(
                parts_dir=tmp / "parts",
                output_path=output_path,
                map_path=map_path,
                force=False,
            )
            self.assertEqual((completed, failed), (1, 0))
            self.assertEqual((tmp / "parts" / "01-chapter" / "001.mp3").read_bytes(), audio_bytes)

    def test_chapter_metadata_uses_epub_titles_and_durations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            one = tmp / "one.mp3"
            two = tmp / "two.mp3"
            one.write_bytes(b"one")
            two.write_bytes(b"two")
            with patch("epub_to_audiobook.cli.ffprobe_duration", side_effect=[1.25, 2.5]):
                metadata_path = write_chapter_metadata_file(
                    chapter_outputs=[
                        ("Chapter = One", one),
                        ("Chapter Two", two),
                    ],
                    metadata_path=tmp / "chapters.ffmetadata",
                )
            text = metadata_path.read_text(encoding="utf-8")
            self.assertIn("START=0", text)
            self.assertIn("END=1250", text)
            self.assertIn("START=1250", text)
            self.assertIn("END=3750", text)
            self.assertIn(f"title={escape_ffmetadata('Chapter = One')}", text)

    def test_media_tool_prefers_path_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_tool = Path(tmpdir) / "ffmpeg"
            fake_tool.write_text("", encoding="utf-8")
            with patch("epub_to_audiobook.cli.shutil.which", return_value=str(fake_tool)):
                self.assertEqual(resolve_media_tool("ffmpeg"), str(fake_tool))

    def test_m4b_merge_embeds_chapter_titles_when_ffmpeg_available(self) -> None:
        try:
            ffmpeg = resolve_media_tool("ffmpeg")
            ffprobe = resolve_media_tool("ffprobe")
        except SystemExit as exc:
            self.skipTest(str(exc))

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            parts_dir = tmp / "parts"
            merge_dir = tmp / "merge"
            parts_dir.mkdir()
            merge_dir.mkdir()
            chapter_outputs = []
            manifest = []
            for idx, title in enumerate(["Chapter One", "Chapter Two"], start=1):
                part_path = parts_dir / f"{idx:03d}.mp3"
                subprocess.run(
                    [
                        ffmpeg,
                        "-y",
                        "-f",
                        "lavfi",
                        "-i",
                        f"sine=frequency={400 + idx * 100}:duration=0.25",
                        "-q:a",
                        "9",
                        "-acodec",
                        "libmp3lame",
                        str(part_path),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                concat_path = merge_dir / f"{idx:02d}.txt"
                concat_path.write_text(f"file '{part_path.as_posix()}'\n", encoding="utf-8")
                chapter_output = tmp / f"{idx:02d}.mp3"
                chapter_outputs.append(chapter_output)
                manifest.append(
                    {
                        "index": idx,
                        "title": title,
                        "chapter_output": chapter_output.as_posix(),
                        "concat_list": concat_path.as_posix(),
                    }
                )

            manifest_path = tmp / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            metadata_path = tmp / "metadata.json"
            metadata_path.write_text(
                json.dumps(
                    {
                        "title": "Test Book",
                        "selection_title": "Test Book",
                        "creator": "Test Author",
                        "publisher": "Test Publisher",
                        "date": "2026",
                        "language": "en",
                        "identifier": "id",
                        "description": "desc",
                        "cover_path": None,
                    }
                ),
                encoding="utf-8",
            )
            book_concat_path = merge_dir / "book.txt"
            book_concat_path.write_text(
                "".join(f"file '{path.as_posix()}'\n" for path in chapter_outputs),
                encoding="utf-8",
            )
            final_path = tmp / "book.m4b"
            merge_all(manifest_path, metadata_path, book_concat_path, final_path, merge_workers=1)
            probe = subprocess.check_output(
                [
                    ffprobe,
                    "-v",
                    "error",
                    "-show_chapters",
                    "-show_entries",
                    "format_tags=title,artist,media_type",
                    "-of",
                    "json",
                    str(final_path),
                ],
                text=True,
            )
            data = json.loads(probe)
            chapters = data.get("chapters", [])
            self.assertEqual([chapter["tags"]["title"] for chapter in chapters], ["Chapter One", "Chapter Two"])
            self.assertEqual(data.get("format", {}).get("tags", {}).get("title"), "Test Book")
            self.assertEqual(data.get("format", {}).get("tags", {}).get("artist"), "Test Author")


if __name__ == "__main__":
    unittest.main()
