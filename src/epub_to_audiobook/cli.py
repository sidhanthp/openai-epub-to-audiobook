from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import time
import mimetypes
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from tempfile import NamedTemporaryFile

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - runtime dependency check
    OpenAI = None


NCX_NS = {"n": "http://www.daisy.org/z3986/2005/ncx/"}
OPF_NS = {
    "o": "http://www.idpf.org/2007/opf",
    "dc": "http://purl.org/dc/elements/1.1/",
}
CONTAINER_NS = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
DEFAULT_SKIP_TITLES = {
    "Welcome",
    "Dedication",
    "Acknowledgments",
    "About the Author",
    "About Twelve",
    "Notes",
    "Copyright",
}
SECTION_ONLY_TITLES = {"WORK", "LOVE", "THE BRAIN AND THE BODY"}
DEFAULT_CLEAN_MODEL = "gpt-5.4-mini"
DEFAULT_TTS_MODEL = "gpt-4o-mini-tts"
DEFAULT_TTS_VOICE = "marin"
DEFAULT_AUDIO_FORMAT = "mp3"
DEFAULT_MAX_CHARS = 3200
DEFAULT_CLEAN_BACKEND = "auto"
DEFAULT_CLEANUP_WORKERS = 4
DEFAULT_CLEANUP_MAX_CHARS = 12000
DEFAULT_CODEX_CLEAN_ATTEMPTS = 2
DEFAULT_CODEX_CLEAN_TIMEOUT_S = 45
DEFAULT_MERGE_WORKERS = 4
DEFAULT_TTS_WORKERS = 1
DEFAULT_TTS_ATTEMPTS = 5
DEFAULT_TTS_SHARD_RETRIES = 4


@dataclass
class EpubMetadata:
    title: str
    creator: str
    publisher: str
    date: str
    language: str
    identifier: str
    description: str
    cover_href: str | None = None
    cover_media_type: str | None = None


@dataclass
class Chapter:
    title: str
    depth: int
    files: list[str]


@dataclass
class PreparedChapter:
    index: int
    title: str
    depth: int
    slug: str
    part_count: int
    raw_path: Path
    clean_path: Path
    parts_dir: Path
    concat_path: Path
    chapter_output: Path


class BlockTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"br", "hr"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {
            "p",
            "div",
            "section",
            "article",
            "blockquote",
            "li",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "h6",
        }:
            self.parts.append("\n\n")

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.parts)
        text = unescape(text).replace("\xa0", " ")
        text = unicodedata.normalize("NFKC", text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Turn an EPUB into one final audiobook audio file using OpenAI cleanup + TTS."
    )
    parser.add_argument("epub", nargs="?", help="Path to the input EPUB")
    parser.add_argument(
        "--out-root",
        default="output/library",
        help="Base output directory (default: output/library)",
    )
    parser.add_argument(
        "--secret-file",
        default=".env",
        help="Env file containing OPENAI_API_KEY (default: .env)",
    )
    parser.add_argument("--voice", default=DEFAULT_TTS_VOICE, help="Built-in OpenAI TTS voice")
    parser.add_argument("--speed", type=float, default=1.0, help="TTS playback speed")
    parser.add_argument(
        "--clean-model",
        default=DEFAULT_CLEAN_MODEL,
        help=f"Cheap text model for cleanup (default: {DEFAULT_CLEAN_MODEL})",
    )
    parser.add_argument(
        "--clean-backend",
        choices=["auto", "codex", "api", "local"],
        default=DEFAULT_CLEAN_BACKEND,
        help="How to run the text cleanup pass (default: auto, which prefers Codex CLI)",
    )
    parser.add_argument(
        "--tts-model",
        default=DEFAULT_TTS_MODEL,
        help=f"TTS model (default: {DEFAULT_TTS_MODEL})",
    )
    parser.add_argument(
        "--audio-format",
        default=DEFAULT_AUDIO_FORMAT,
        choices=["mp3", "flac", "wav"],
        help=f"Output format for rendered chunk and final audio files (default: {DEFAULT_AUDIO_FORMAT})",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=DEFAULT_MAX_CHARS,
        help=f"Max chars per TTS chunk (default: {DEFAULT_MAX_CHARS})",
    )
    parser.add_argument(
        "--rpm",
        type=int,
        default=50,
        help="Requests per minute cap for TTS batch generation",
    )
    parser.add_argument(
        "--instructions",
        help="Override voice instructions. If omitted, a sensible audiobook default is used.",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip the cheap-model cleanup pass and use only local text cleanup",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Prepare text/jobs/manifests but do not call TTS or ffmpeg",
    )
    parser.add_argument(
        "--render-dir",
        help="Resume from an existing prepared render directory containing jobs.jsonl/manifest.json/metadata.json",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing audio outputs",
    )
    parser.add_argument(
        "--skip-title",
        action="append",
        default=[],
        help="Additional TOC titles to skip. Can be passed multiple times.",
    )
    parser.add_argument(
        "--list-chapters",
        action="store_true",
        help="List parsed chapters and exit",
    )
    parser.add_argument(
        "--chapter-number",
        type=int,
        action="append",
        default=[],
        help="Only include specific 1-based chapter numbers. Can be passed multiple times.",
    )
    parser.add_argument(
        "--chapter-match",
        action="append",
        default=[],
        help="Only include chapters whose titles contain this case-insensitive substring.",
    )
    parser.add_argument(
        "--cleanup-workers",
        type=int,
        default=DEFAULT_CLEANUP_WORKERS,
        help=f"Parallel workers for chapter/chunk cleanup (default: {DEFAULT_CLEANUP_WORKERS})",
    )
    parser.add_argument(
        "--cleanup-max-chars",
        type=int,
        default=DEFAULT_CLEANUP_MAX_CHARS,
        help=f"Max chars per cleanup chunk before TTS chunking (default: {DEFAULT_CLEANUP_MAX_CHARS})",
    )
    parser.add_argument(
        "--merge-workers",
        type=int,
        default=DEFAULT_MERGE_WORKERS,
        help=f"Parallel workers for chapter MP3 merges (default: {DEFAULT_MERGE_WORKERS})",
    )
    parser.add_argument(
        "--tts-workers",
        type=int,
        default=DEFAULT_TTS_WORKERS,
        help=f"Parallel workers for TTS shard generation (default: {DEFAULT_TTS_WORKERS})",
    )
    parser.add_argument(
        "--tts-attempts",
        type=int,
        default=DEFAULT_TTS_ATTEMPTS,
        help=f"Per-request retry attempts passed to the speech CLI (default: {DEFAULT_TTS_ATTEMPTS})",
    )
    parser.add_argument(
        "--tts-shard-retries",
        type=int,
        default=DEFAULT_TTS_SHARD_RETRIES,
        help=f"Outer retry count for each TTS shard with random exponential backoff (default: {DEFAULT_TTS_SHARD_RETRIES})",
    )
    return parser.parse_args()


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "book"


def load_api_key(secret_file: Path) -> None:
    if os.getenv("OPENAI_API_KEY"):
        return
    if not secret_file.exists():
        raise SystemExit(f"Missing secret file: {secret_file}")
    for line in secret_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("OPENAI_API_KEY="):
            os.environ["OPENAI_API_KEY"] = line.split("=", 1)[1].strip()
            return
    raise SystemExit(f"OPENAI_API_KEY not found in {secret_file}")


def resolve_epub_paths(zf: zipfile.ZipFile) -> tuple[str, str]:
    container_path = "META-INF/container.xml"
    opf_path = "content.opf"
    if container_path in zf.namelist():
        container = ET.fromstring(zf.read(container_path))
        rootfile = container.find(".//c:rootfile", CONTAINER_NS)
        if rootfile is not None:
            opf_path = rootfile.attrib.get("full-path", opf_path)
    opf_dir = str(Path(opf_path).parent)
    if opf_dir == ".":
        opf_dir = ""
    return opf_path, opf_dir


def resolve_href(base_dir: str, href: str) -> str:
    if not base_dir:
        return str(Path(href))
    return str(Path(base_dir) / href)


def read_opf_metadata(zf: zipfile.ZipFile) -> tuple[EpubMetadata, dict[str, str], list[str], str | None]:
    opf_path, opf_dir = resolve_epub_paths(zf)
    opf = ET.fromstring(zf.read(opf_path))
    manifest = {}
    cover_href = None
    cover_media_type = None
    cover_id = None
    for meta in opf.findall(".//o:metadata/o:meta", OPF_NS):
        if meta.attrib.get("name") == "cover":
            cover_id = meta.attrib.get("content")
            break
    for item in opf.findall(".//o:manifest/o:item", OPF_NS):
        manifest[item.attrib["id"]] = resolve_href(opf_dir, item.attrib["href"])
        properties = set(item.attrib.get("properties", "").split())
        media_type = item.attrib.get("media-type")
        if "cover-image" in properties:
            cover_href = manifest[item.attrib["id"]]
            cover_media_type = media_type
        elif cover_id and item.attrib["id"] == cover_id:
            cover_href = manifest[item.attrib["id"]]
            cover_media_type = media_type
        elif not cover_href and media_type and media_type.startswith("image/"):
            href = item.attrib.get("href", "")
            item_id = item.attrib.get("id", "").lower()
            if "cover" in item_id or "cover" in href.lower():
                cover_href = manifest[item.attrib["id"]]
                cover_media_type = media_type
    spine = [manifest[item.attrib["idref"]] for item in opf.findall(".//o:spine/o:itemref", OPF_NS)]
    metadata = EpubMetadata(
        title=(opf.findtext(".//dc:title", default="", namespaces=OPF_NS) or "").strip(),
        creator=(opf.findtext(".//dc:creator", default="", namespaces=OPF_NS) or "").strip(),
        publisher=(opf.findtext(".//dc:publisher", default="", namespaces=OPF_NS) or "").strip(),
        date=(opf.findtext(".//dc:date", default="", namespaces=OPF_NS) or "").strip(),
        language=(opf.findtext(".//dc:language", default="", namespaces=OPF_NS) or "").strip(),
        identifier=(opf.findtext(".//dc:identifier", default="", namespaces=OPF_NS) or "").strip(),
        description=(opf.findtext(".//dc:description", default="", namespaces=OPF_NS) or "").strip(),
        cover_href=cover_href,
        cover_media_type=cover_media_type,
    )
    toc_id = opf.find(".//o:spine", OPF_NS)
    toc_path = None
    if toc_id is not None and toc_id.attrib.get("toc"):
        toc_manifest_id = toc_id.attrib["toc"]
        toc_path = manifest.get(toc_manifest_id)
    return metadata, manifest, spine, toc_path


def extract_points(root: ET.Element, toc_dir: str = "") -> list[dict[str, object]]:
    points: list[dict[str, object]] = []

    def walk(node: ET.Element, depth: int = 0) -> None:
        label_node = node.find("n:navLabel/n:text", NCX_NS)
        content_node = node.find("n:content", NCX_NS)
        if label_node is None or content_node is None or not label_node.text:
            return
        src = resolve_href(toc_dir, content_node.attrib["src"].split("#", 1)[0])
        points.append({"title": label_node.text.strip(), "src": src, "depth": depth})
        for child in node.findall("n:navPoint", NCX_NS):
            walk(child, depth + 1)

    for node in root.findall("n:navMap/n:navPoint", NCX_NS):
        walk(node)
    return points


def derive_fallback_title(href: str, text: str, index: int) -> str:
    for line in text.split("\n"):
        line = line.strip()
        if 2 <= len(line) <= 120:
            return line
    return f"Chapter {index:02d} ({Path(href).stem})"


def parse_epub(epub_path: Path, skip_titles: set[str]) -> tuple[EpubMetadata, list[Chapter], dict[str, str]]:
    with zipfile.ZipFile(epub_path) as zf:
        metadata, _manifest, spine, toc_path = read_opf_metadata(zf)
        spine_index = {href: idx for idx, href in enumerate(spine)}

        texts_by_href: dict[str, str] = {}
        for href in spine:
            if not href.endswith((".html", ".xhtml")):
                continue
            parser = BlockTextExtractor()
            parser.feed(zf.read(href).decode("utf-8", "ignore"))
            text = parser.get_text()
            if text:
                texts_by_href[href] = text

        points: list[dict[str, object]] = []
        if toc_path and toc_path in zf.namelist():
            toc = ET.fromstring(zf.read(toc_path))
            toc_dir = str(Path(toc_path).parent)
            if toc_dir == ".":
                toc_dir = ""
            points = [p for p in extract_points(toc, toc_dir) if p["src"] in spine_index]

        chapters: list[Chapter] = []
        if points:
            starts = [spine_index[p["src"]] for p in points]
            filtered = [p for p in points if str(p["title"]) not in skip_titles]
            for point in filtered:
                start = spine_index[str(point["src"])]
                later_starts = [idx for idx in starts if idx > start]
                end = min(later_starts) if later_starts else len(spine)
                files = [
                    href
                    for href in spine[start:end]
                    if href.endswith((".html", ".xhtml")) and href in texts_by_href
                ]
                if files:
                    chapters.append(Chapter(title=str(point["title"]), depth=int(point["depth"]), files=files))
        else:
            for idx, href in enumerate(spine, start=1):
                if href not in texts_by_href:
                    continue
                title = derive_fallback_title(href, texts_by_href[href], idx)
                chapters.append(Chapter(title=title, depth=0, files=[href]))

        texts_by_title: dict[str, str] = {}
        for chapter in chapters:
            pieces = [texts_by_href[href] for href in chapter.files if href in texts_by_href]
            texts_by_title[chapter.title] = "\n\n".join(pieces).strip()

        return metadata, chapters, texts_by_title


def select_chapters(
    chapters: list[Chapter],
    raw_texts: dict[str, str],
    chapter_numbers: list[int],
    chapter_matches: list[str],
) -> tuple[list[Chapter], dict[str, str]]:
    if not chapter_numbers and not chapter_matches:
        return chapters, raw_texts

    wanted_numbers = set(chapter_numbers)
    wanted_matches = [item.lower() for item in chapter_matches]
    selected: list[Chapter] = []
    for idx, chapter in enumerate(chapters, start=1):
        number_ok = not wanted_numbers or idx in wanted_numbers
        match_ok = not wanted_matches or any(term in chapter.title.lower() for term in wanted_matches)
        if number_ok and match_ok:
            selected.append(chapter)

    if not selected:
        raise SystemExit("No chapters matched the requested filters.")

    selected_texts = {chapter.title: raw_texts[chapter.title] for chapter in selected}
    return selected, selected_texts


def default_instructions(speed: float) -> str:
    return "\n".join(
        [
            "Voice Affect: Warm, intimate, polished audiobook narration for adult listeners.",
            "Tone: Natural, understated, and pleasant for long-form listening.",
            f"Pacing: Steady medium-slow narration designed to remain easy to follow at {speed}x output speed.",
            "Emotion: Use subtle emotional color only when the text clearly calls for it.",
            "Pronunciation: Keep names and repeated terms pronounced consistently across all chunks.",
            "Pauses: Add short natural pauses at commas and sentence endings, with slightly longer pauses at paragraph breaks and scene transitions.",
            "Delivery: Avoid sounding salesy, rushed, overly cheerful, breathless, robotic, or theatrical.",
        ]
    )


def strip_leading_repeat(text: str, repeated: str) -> str:
    if not repeated:
        return text
    lines = text.split("\n")
    while lines and lines[0].strip() == repeated:
        lines.pop(0)
    return "\n".join(lines).strip()


def local_cleanup(book_title: str, chapter_title: str, text: str) -> str:
    text = strip_leading_repeat(text, book_title)
    text = strip_leading_repeat(text, chapter_title)
    text = re.sub(r"\bBegin Reading\b", "", text, flags=re.I)
    text = re.sub(r"\bTable of Contents\b", "", text, flags=re.I)
    text = re.sub(r"\bCopyright Page\b", "", text, flags=re.I)
    text = re.sub(
        r"In accordance with the U\.S\. Copyright Act of 1976,.*?author’s intellectual property\.",
        "",
        text,
        flags=re.I | re.S,
    )
    text = re.sub(r"\bAUTHOR.?S NOTE\b", "Author's Note", text, flags=re.I)
    text = re.sub(r"\bPREFACE\b", "Preface", text, flags=re.I)
    text = re.sub(r"\bINTRODUCTION\b", "Introduction", text, flags=re.I)
    text = re.sub(r"\bEPILOGUE\b", "Epilogue", text, flags=re.I)
    text = re.sub(r"(?m)^[ \t]*\[[^\]]+\][ \t]*$", "", text)
    text = re.sub(r"(?m)^[ \t]*$", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"([A-Za-z])\n([a-z])", r"\1 \2", text)
    text = text.strip()
    if chapter_title not in SECTION_ONLY_TITLES and text.startswith(chapter_title):
        text = text[len(chapter_title) :].lstrip(" :\n")
    chapter_prefix = chapter_title + "\n\n"
    if chapter_title in SECTION_ONLY_TITLES:
        text = ""
    return (chapter_prefix + text).strip()


def chunk_text(text: str, max_chars: int) -> list[str]:
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    current = ""
    for para in paragraphs:
        candidate = para if not current else current + "\n\n" + para
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(para) <= max_chars:
            current = para
            continue
        sentences = re.split(r"(?<=[.!?])\s+", para)
        sentence_block = ""
        for sentence in sentences:
            candidate = sentence if not sentence_block else sentence_block + " " + sentence
            if len(candidate) <= max_chars:
                sentence_block = candidate
            else:
                if sentence_block:
                    chunks.append(sentence_block)
                sentence_block = sentence
        if sentence_block:
            current = sentence_block
    if current:
        chunks.append(current)
    return chunks


def cleanup_prompt(chapter_title: str, text: str, index: int, total: int) -> str:
    return (
        "You clean ebook extraction text. Return only the cleaned text with no preamble.\n"
        "Preserve wording, sentence order, paragraph order, and factual content exactly.\n"
        "Allowed changes only:\n"
        "- remove obvious extraction artifacts or random stray words or tokens\n"
        "- fix broken drop-cap spacing such as 'I n' to 'In'\n"
        "- remove duplicated running headers already reflected by the chapter title\n"
        "- normalize accidental whitespace and spacing before punctuation\n"
        "Do not summarize, rewrite, modernize, add content, or remove meaningful content.\n\n"
        f"Chapter: {chapter_title}\n"
        f"Chunk {index} of {total}\n\n"
        f"{text}"
    )


def _length_guard(original: str, cleaned: str) -> str:
    original_len = len(original)
    cleaned_len = len(cleaned)
    if cleaned_len < original_len * 0.7 or cleaned_len > original_len * 1.1:
        return original
    return cleaned


def clean_chunk_api(client: OpenAI, model: str, chapter_title: str, text: str, index: int, total: int) -> str:
    system = (
        "You clean ebook extraction text. Return only cleaned text.\n"
        "Preserve wording, sentence order, paragraph order, and factual content exactly.\n"
        "Allowed changes only:\n"
        "- remove obvious extraction artifacts or random stray words or tokens\n"
        "- fix broken drop-cap spacing such as 'I n' to 'In'\n"
        "- remove duplicated running headers already reflected by the chapter title\n"
        "- normalize accidental whitespace and spacing before punctuation\n"
        "Do not summarize, rewrite, modernize, or add content."
    )
    user = f"Chapter: {chapter_title}\nChunk {index} of {total}\n\n{text}"
    response = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    cleaned = response.choices[0].message.content.strip()
    return _length_guard(text, cleaned)


def codex_cli_path() -> Path:
    found = shutil.which("codex")
    if found:
        return Path(found)
    return Path.home() / ".nvm" / "versions" / "node" / "v22.19.0" / "bin" / "codex"


def codex_logged_in() -> bool:
    cli = codex_cli_path()
    if not cli.exists():
        return False
    result = subprocess.run(
        [str(cli), "login", "status"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def choose_clean_backend(requested: str, use_clean_model: bool) -> str:
    if not use_clean_model:
        return "local"
    if requested != "auto":
        return requested
    if codex_logged_in():
        return "codex"
    return "api"


def media_type_to_suffix(media_type: str | None, href: str | None) -> str:
    if media_type:
        guessed = mimetypes.guess_extension(media_type, strict=False)
        if guessed:
            return guessed
    if href:
        suffix = Path(href).suffix
        if suffix:
            return suffix
    return ".img"


def extract_cover_file(epub_path: Path, metadata: EpubMetadata, assets_dir: Path) -> Path | None:
    if not metadata.cover_href:
        return None
    assets_dir.mkdir(parents=True, exist_ok=True)
    suffix = media_type_to_suffix(metadata.cover_media_type, metadata.cover_href)
    cover_path = assets_dir / f"cover{suffix}"
    with zipfile.ZipFile(epub_path) as zf:
        if metadata.cover_href not in zf.namelist():
            return None
        cover_path.write_bytes(zf.read(metadata.cover_href))
    return cover_path


def html_fragment_to_text(fragment: str) -> str:
    if not fragment:
        return ""
    parser = BlockTextExtractor()
    parser.feed(fragment)
    return re.sub(r"\s+", " ", parser.get_text()).strip()


def truncate_plaintext(value: str, limit: int = 240) -> str:
    plain = re.sub(r"\s+", " ", value).strip()
    if len(plain) <= limit:
        return plain
    clipped = plain[: limit - 3].rsplit(" ", 1)[0].strip()
    return (clipped or plain[: limit - 3]).strip() + "..."


def clean_metadata_value(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\x00", " ")).strip()


def metadata_year(date_value: str) -> str:
    match = re.search(r"\b(\d{4})\b", date_value)
    return match.group(1) if match else ""


def final_render_title(book_title: str, selection_slug: str, chapters: list[Chapter]) -> str:
    if selection_slug == "full-book":
        return book_title
    if len(chapters) == 1:
        return chapters[0].title
    return f"{book_title} (Selection)"


def build_audio_metadata(
    epub_metadata: EpubMetadata,
    *,
    title: str,
    album: str,
    track_number: int | None = None,
    track_total: int | None = None,
) -> dict[str, str]:
    description = truncate_plaintext(html_fragment_to_text(epub_metadata.description))
    tags = {
        "title": clean_metadata_value(title),
        "album": clean_metadata_value(album),
        "artist": clean_metadata_value(epub_metadata.creator),
        "album_artist": clean_metadata_value(epub_metadata.creator),
        "composer": clean_metadata_value(epub_metadata.creator),
        "genre": "Audiobook",
        "publisher": clean_metadata_value(epub_metadata.publisher),
        "date": clean_metadata_value(epub_metadata.date),
        "year": metadata_year(epub_metadata.date),
        "language": clean_metadata_value(epub_metadata.language),
        "isbn": clean_metadata_value(epub_metadata.identifier),
        "description": clean_metadata_value(description),
        "comment": clean_metadata_value(description),
    }
    if track_number is not None and track_total:
        tags["track"] = f"{track_number}/{track_total}"
    return {key: value for key, value in tags.items() if value}


def clean_chunk_codex(model: str, chapter_title: str, text: str, index: int, total: int, workdir: Path) -> str:
    cli = codex_cli_path()
    if not cli.exists():
        raise SystemExit(f"Codex CLI not found: {cli}")
    prompt = cleanup_prompt(chapter_title, text, index, total)
    with NamedTemporaryFile("w+", encoding="utf-8", suffix=".txt", delete=False) as out_file:
        out_path = Path(out_file.name)
    try:
        cmd = [
            str(cli),
            "exec",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--model",
            model,
            "--output-last-message",
            str(out_path),
            "-",
        ]
        last_error: Exception | None = None
        for attempt in range(1, DEFAULT_CODEX_CLEAN_ATTEMPTS + 1):
            try:
                subprocess.run(
                    cmd,
                    input=prompt,
                    text=True,
                    cwd=str(workdir),
                    check=True,
                    timeout=DEFAULT_CODEX_CLEAN_TIMEOUT_S,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                cleaned = out_path.read_text(encoding="utf-8").strip()
                return _length_guard(text, cleaned)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                last_error = exc
                time.sleep(min(4.0, 0.75 * attempt) + random.uniform(0.0, 0.4))
        if isinstance(last_error, subprocess.CalledProcessError):
            raise last_error
        if isinstance(last_error, subprocess.TimeoutExpired):
            raise RuntimeError(f"Codex cleanup timed out after {DEFAULT_CODEX_CLEAN_TIMEOUT_S}s")
        raise RuntimeError("Codex cleanup failed without an exception")
    finally:
        out_path.unlink(missing_ok=True)


def _clean_one_chunk(
    *,
    active_clean_backend: str,
    client: OpenAI | None,
    clean_model: str,
    chapter_title: str,
    chapter_slug: str,
    chunk: str,
    chunk_idx: int,
    total_chunks: int,
    workdir: Path,
) -> tuple[int, str]:
    if active_clean_backend == "local":
        cleaned = chunk
    elif active_clean_backend == "api":
        print(f"Cleaning {chapter_slug} chunk {chunk_idx}/{total_chunks}")
        cleaned = clean_chunk_api(client, clean_model, chapter_title, chunk, chunk_idx, total_chunks)
    else:
        print(f"Cleaning {chapter_slug} chunk {chunk_idx}/{total_chunks} with Codex CLI")
        try:
            cleaned = clean_chunk_codex(clean_model, chapter_title, chunk, chunk_idx, total_chunks, workdir)
        except Exception:
            print(
                f"Codex cleanup failed for {chapter_slug} chunk {chunk_idx}/{total_chunks}; falling back to local cleanup",
                file=sys.stderr,
            )
            cleaned = chunk
    return chunk_idx, cleaned


def prepare_chapter(
    *,
    chapter_index: int,
    chapter: Chapter,
    book_title: str,
    chapter_text: str,
    raw_dir: Path,
    clean_dir: Path,
    parts_dir: Path,
    merge_dir: Path,
    voice: str,
    speed: float,
    audio_format: str,
    instructions: str,
    max_chars: int,
    cleanup_max_chars: int,
    active_clean_backend: str,
    clean_model: str,
    client: OpenAI | None,
    workdir: Path,
    cleanup_workers: int,
) -> tuple[PreparedChapter, list[dict[str, object]]]:
    chapter_slug = f"{chapter_index:02d}-{slugify(chapter.title)}"
    raw_text = local_cleanup(book_title, chapter.title, chapter_text)
    raw_path = raw_dir / f"{chapter_slug}.txt"
    raw_path.write_text(raw_text + "\n", encoding="utf-8")

    cleanup_chunks = chunk_text(raw_text, cleanup_max_chars)
    chunk_tasks = []
    for chunk_idx, chunk in enumerate(cleanup_chunks, start=1):
        chunk_tasks.append(
            {
                "active_clean_backend": active_clean_backend,
                "client": client,
                "clean_model": clean_model,
                "chapter_title": chapter.title,
                "chapter_slug": chapter_slug,
                "chunk": chunk,
                "chunk_idx": chunk_idx,
                "total_chunks": len(cleanup_chunks),
                "workdir": workdir,
            }
        )

    clean_chunks_by_index: dict[int, str] = {}
    chunk_worker_count = max(1, min(cleanup_workers, len(chunk_tasks)))
    if chunk_worker_count == 1:
        for task in chunk_tasks:
            chunk_idx, cleaned = _clean_one_chunk(**task)
            clean_chunks_by_index[chunk_idx] = cleaned
    else:
        with ThreadPoolExecutor(max_workers=chunk_worker_count) as executor:
            futures = [executor.submit(_clean_one_chunk, **task) for task in chunk_tasks]
            for future in futures:
                chunk_idx, cleaned = future.result()
                clean_chunks_by_index[chunk_idx] = cleaned

    cleaned_cleanup_chunks = [clean_chunks_by_index[idx] for idx in sorted(clean_chunks_by_index)]
    clean_text = "\n\n".join(cleaned_cleanup_chunks).strip()
    clean_path = clean_dir / f"{chapter_slug}.txt"
    clean_path.write_text(clean_text + "\n", encoding="utf-8")

    tts_chunks = chunk_text(clean_text, max_chars)

    chapter_parts_dir = parts_dir / chapter_slug
    chapter_parts_dir.mkdir(parents=True, exist_ok=True)
    concat_path = merge_dir / f"{chapter_slug}.txt"
    jobs: list[dict[str, object]] = []
    with concat_path.open("w", encoding="utf-8") as concat_file:
        for part_idx, chunk in enumerate(tts_chunks, start=1):
            relative_out = f"{chapter_slug}/{part_idx:03d}.{audio_format}"
            jobs.append(
                {
                    "input": chunk,
                    "voice": voice,
                    "speed": speed,
                    "instructions": instructions,
                    "response_format": audio_format,
                    "out": relative_out,
                }
            )
            part_path = chapter_parts_dir / f"{part_idx:03d}.{audio_format}"
            concat_file.write(f"file '{part_path.as_posix()}'\n")

    prepared = PreparedChapter(
        index=chapter_index,
        title=chapter.title,
        depth=chapter.depth,
        slug=chapter_slug,
        part_count=len(tts_chunks),
        raw_path=raw_path,
        clean_path=clean_path,
        parts_dir=chapter_parts_dir,
        concat_path=concat_path,
        chapter_output=merge_dir.parent / f"{chapter_slug}.{audio_format}",
    )
    return prepared, jobs


def prepare_outputs(
    *,
    epub_path: Path,
    out_root: Path,
    voice: str,
    speed: float,
    audio_format: str,
    tts_model: str,
    clean_model: str,
    instructions: str,
    max_chars: int,
    cleanup_max_chars: int,
    skip_titles: set[str],
    use_clean_model: bool,
    clean_backend: str,
    cleanup_workers: int,
    chapter_numbers: list[int],
    chapter_matches: list[str],
) -> dict[str, object]:
    metadata, chapters, raw_texts = parse_epub(epub_path, skip_titles)
    chapters, raw_texts = select_chapters(chapters, raw_texts, chapter_numbers, chapter_matches)
    book_slug = slugify(epub_path.stem)
    selection_slug = "full-book"
    if len(chapters) == 1:
        selection_slug = slugify(chapters[0].title)
    elif chapter_numbers or chapter_matches:
        selection_slug = "selection"
    base_dir = out_root / book_slug / selection_slug
    raw_dir = base_dir / "raw_text"
    clean_dir = base_dir / "clean_text"
    parts_dir = base_dir / "parts"
    merge_dir = base_dir / "merge"
    assets_dir = base_dir / "assets"
    jobs_path = base_dir / "jobs.jsonl"
    manifest_path = base_dir / "manifest.json"
    metadata_path = base_dir / "metadata.json"
    book_concat_path = merge_dir / "book.txt"
    final_basename = book_slug if selection_slug == "full-book" else f"{book_slug}-{selection_slug}"
    final_book_path = base_dir / f"{final_basename}.{audio_format}"

    for path in [raw_dir, clean_dir, parts_dir, merge_dir, assets_dir]:
        path.mkdir(parents=True, exist_ok=True)

    active_clean_backend = choose_clean_backend(clean_backend, use_clean_model)
    if active_clean_backend == "api" and OpenAI is None:
        raise SystemExit("openai SDK not installed. Install it before running API cleanup.")
    client = OpenAI() if active_clean_backend == "api" else None
    prepared_results: list[tuple[PreparedChapter, list[dict[str, object]]]] = []
    chapter_tasks = []
    for idx, chapter in enumerate(chapters, start=1):
        chapter_tasks.append(
            {
                "chapter_index": idx,
                "chapter": chapter,
                "book_title": metadata.title,
                "chapter_text": raw_texts[chapter.title],
                "raw_dir": raw_dir,
                "clean_dir": clean_dir,
                "parts_dir": parts_dir,
                "merge_dir": merge_dir,
                "voice": voice,
                "speed": speed,
                "audio_format": audio_format,
                "instructions": instructions,
                "max_chars": max_chars,
                "cleanup_max_chars": cleanup_max_chars,
                "active_clean_backend": active_clean_backend,
                "clean_model": clean_model,
                "client": client,
                "workdir": epub_path.parent,
                "cleanup_workers": cleanup_workers,
            }
        )

    chapter_worker_count = max(1, min(cleanup_workers, len(chapter_tasks)))
    if chapter_worker_count == 1:
        for task in chapter_tasks:
            prepared_results.append(prepare_chapter(**task))
    else:
        with ThreadPoolExecutor(max_workers=chapter_worker_count) as executor:
            futures = [executor.submit(prepare_chapter, **task) for task in chapter_tasks]
            for future in futures:
                prepared_results.append(future.result())

    prepared_results.sort(key=lambda item: item[0].index)
    manifest: list[dict[str, object]] = []
    total_parts = 0
    with jobs_path.open("w", encoding="utf-8") as jobs_file:
        for prepared, jobs in prepared_results:
            for job in jobs:
                jobs_file.write(json.dumps(job, ensure_ascii=False) + "\n")
            total_parts += prepared.part_count
            manifest.append(
                {
                    "index": prepared.index,
                    "title": prepared.title,
                    "depth": prepared.depth,
                    "slug": prepared.slug,
                    "part_count": prepared.part_count,
                    "raw_text": prepared.raw_path.as_posix(),
                    "clean_text": prepared.clean_path.as_posix(),
                    "parts_dir": prepared.parts_dir.as_posix(),
                    "concat_list": prepared.concat_path.as_posix(),
                    "chapter_output": prepared.chapter_output.as_posix(),
                }
            )

    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    cover_path = extract_cover_file(epub_path, metadata, assets_dir)
    selection_title = final_render_title(metadata.title, selection_slug, chapters)
    metadata_doc = {
        "source_epub": epub_path.as_posix(),
        "book_slug": book_slug,
        "selection_slug": selection_slug,
        "selection_title": selection_title,
        "title": metadata.title,
        "creator": metadata.creator,
        "publisher": metadata.publisher,
        "date": metadata.date,
        "language": metadata.language,
        "identifier": metadata.identifier,
        "description": truncate_plaintext(html_fragment_to_text(metadata.description), limit=500),
        "cover_path": cover_path.as_posix() if cover_path else None,
        "audio_format": audio_format,
        "voice": voice,
        "speed": speed,
        "tts_model": tts_model,
        "clean_model": clean_model if use_clean_model else None,
    }
    metadata_path.write_text(json.dumps(metadata_doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    with book_concat_path.open("w", encoding="utf-8") as book_concat:
        for item in manifest:
            book_concat.write(f"file '{item['chapter_output']}'\n")

    print(f"Prepared {len(manifest)} chapters and {total_parts} audio chunks.")
    print(f"Jobs: {jobs_path}")
    print(f"Manifest: {manifest_path}")
    print(f"Final audio target: {final_book_path}")

    return {
        "book_title": metadata.title,
        "creator": metadata.creator,
        "base_dir": base_dir,
        "jobs_path": jobs_path,
        "manifest_path": manifest_path,
        "metadata_path": metadata_path,
        "book_concat_path": book_concat_path,
        "final_book_path": final_book_path,
        "clean_backend": active_clean_backend,
    }


def run_tts_batch(
    *,
    jobs_path: Path,
    out_dir: Path,
    tts_model: str,
    voice: str,
    speed: float,
    audio_format: str,
    instructions: str,
    rpm: int,
    tts_workers: int,
    tts_attempts: int,
    tts_shard_retries: int,
    force: bool,
) -> None:
    def load_jobs_jsonl(path: Path) -> list[dict[str, object]]:
        jobs: list[dict[str, object]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if raw:
                jobs.append(json.loads(raw))
        return jobs

    def job_output_path(job: dict[str, object], shard_out_dir: Path, index: int) -> Path:
        explicit_out = job.get("out")
        if explicit_out:
            return shard_out_dir / Path(str(explicit_out))
        return shard_out_dir / f"{index:03d}.{audio_format}"

    def tts_error_is_retryable(text: str) -> bool:
        lowered = text.lower()
        if (
            "insufficient_quota" in lowered
            or "current quota" in lowered
            or "billing details" in lowered
        ):
            return False
        return (
            "429" in lowered
            or "rate limit" in lowered
            or "too many requests" in lowered
            or "retrying in" in lowered
            or "timeout" in lowered
            or "timed out" in lowered
            or "connection reset" in lowered
        )

    def build_tts_base_cmd(shard_rpm: int, shard_force: bool) -> list[str]:
        uv_path = shutil.which("uv")
        if uv_path:
            cmd = [uv_path, "run", "--with", "openai", "python", "-m", "epub_to_audiobook.tts_batch"]
        else:
            cmd = [sys.executable, "-m", "epub_to_audiobook.tts_batch"]
        cmd.extend(
            [
                "speak-batch",
                "--model",
                tts_model,
                "--voice",
                voice,
                "--speed",
                str(speed),
                "--response-format",
                audio_format,
                "--instructions",
                instructions,
                "--attempts",
                str(tts_attempts),
                "--rpm",
                str(shard_rpm),
            ]
        )
        if shard_force:
            cmd.append("--force")
        return cmd

    def split_jobs_for_workers(jobs: list[dict[str, object]], workers: int) -> list[list[dict[str, object]]]:
        shards = [[] for _ in range(workers)]
        for idx, job in enumerate(jobs):
            shards[idx % workers].append(job)
        return [shard for shard in shards if shard]

    def run_tts_shard(
        *,
        shard_index: int,
        shard_jobs: list[dict[str, object]],
        shard_out_dir: Path,
        shard_rpm: int,
        max_retries: int,
        shard_force: bool,
        temp_dir: Path,
    ) -> None:
        if not shard_jobs:
            return

        if shard_index > 0:
            time.sleep(random.uniform(0.1, 0.6) * shard_index)

        for attempt in range(1, max_retries + 1):
            pending_jobs: list[dict[str, object]] = []
            for idx, job in enumerate(shard_jobs, start=1):
                output_path = job_output_path(job, shard_out_dir, idx)
                if shard_force and attempt == 1:
                    pending_jobs.append(job)
                elif not output_path.exists():
                    pending_jobs.append(job)

            if not pending_jobs:
                return

            shard_jobs_path = temp_dir / f"shard-{shard_index + 1:02d}.jsonl"
            shard_jobs_path.write_text(
                "".join(json.dumps(job, ensure_ascii=False) + "\n" for job in pending_jobs),
                encoding="utf-8",
            )

            cmd = build_tts_base_cmd(shard_rpm=shard_rpm, shard_force=shard_force and attempt == 1)
            cmd.extend(["--input", str(shard_jobs_path), "--out-dir", str(shard_out_dir)])
            result = subprocess.run(cmd, text=True, capture_output=True)
            combined_output = "\n".join(part for part in [result.stdout, result.stderr] if part).strip()
            if result.returncode == 0:
                return
            if attempt >= max_retries or not tts_error_is_retryable(combined_output):
                raise subprocess.CalledProcessError(
                    result.returncode,
                    cmd,
                    output=result.stdout,
                    stderr=result.stderr,
                )

            sleep_s = min(60.0, 2.0 ** attempt) + random.uniform(0.0, 1.0)
            print(
                f"TTS shard {shard_index + 1} attempt {attempt}/{max_retries} failed; retrying in {sleep_s:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_s)

        raise RuntimeError(f"TTS shard {shard_index + 1} exhausted retries")

    jobs = load_jobs_jsonl(jobs_path)
    worker_count = max(1, min(tts_workers, len(jobs), rpm))
    shard_rpm = max(1, math.floor(rpm / worker_count))
    shards = split_jobs_for_workers(jobs, worker_count)
    temp_dir = out_dir / "_tts_shards"
    temp_dir.mkdir(parents=True, exist_ok=True)

    if worker_count == 1:
        run_tts_shard(
            shard_index=0,
            shard_jobs=shards[0],
            shard_out_dir=out_dir,
            shard_rpm=shard_rpm,
            max_retries=tts_shard_retries,
            shard_force=force,
            temp_dir=temp_dir,
        )
        return

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                run_tts_shard,
                shard_index=idx,
                shard_jobs=shard,
                shard_out_dir=out_dir,
                shard_rpm=shard_rpm,
                max_retries=tts_shard_retries,
                shard_force=force,
                temp_dir=temp_dir,
            )
            for idx, shard in enumerate(shards)
        ]
        for future in futures:
            future.result()


def merge_audio(
    concat_path: Path,
    output_path: Path,
    *,
    audio_metadata: dict[str, str] | None = None,
    cover_path: Path | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    if suffix not in {".mp3", ".flac", ".wav"}:
        raise SystemExit(f"Unsupported merged audio format: {output_path.suffix}")
    cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat_path)]
    if cover_path and suffix == ".mp3" and cover_path.exists():
        cmd.extend(["-i", str(cover_path)])
    cmd.extend(["-map_metadata", "-1", "-map", "0:a", "-codec:a", "copy"])
    if cover_path and suffix == ".mp3" and cover_path.exists():
        cmd.extend(
            [
                "-map",
                "1:v",
                "-c:v",
                "mjpeg",
                "-disposition:v:0",
                "attached_pic",
                "-id3v2_version",
                "3",
                "-metadata:s:v",
                "title=Cover",
                "-metadata:s:v",
                "comment=Cover (front)",
            ]
        )
    if audio_metadata:
        for key, value in audio_metadata.items():
            cmd.extend(["-metadata", f"{key}={value}"])
    cmd.append(str(output_path))
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def merge_all(
    manifest_path: Path,
    metadata_path: Path,
    book_concat_path: Path,
    final_book_path: Path,
    merge_workers: int,
) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    render_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    epub_metadata = EpubMetadata(
        title=render_metadata.get("title", ""),
        creator=render_metadata.get("creator", ""),
        publisher=render_metadata.get("publisher", ""),
        date=render_metadata.get("date", ""),
        language=render_metadata.get("language", ""),
        identifier=render_metadata.get("identifier", ""),
        description=render_metadata.get("description", ""),
    )
    cover_path = Path(render_metadata["cover_path"]) if render_metadata.get("cover_path") else None
    chapter_total = len(manifest)

    def merge_one(item: dict[str, object]) -> str:
        chapter_title = str(item["title"])
        merge_audio(
            Path(item["concat_list"]),
            Path(item["chapter_output"]),
            audio_metadata=build_audio_metadata(
                epub_metadata,
                title=chapter_title,
                album=render_metadata.get("title", epub_metadata.title),
                track_number=int(item["index"]),
                track_total=chapter_total,
            ),
            cover_path=cover_path,
        )
        return str(item["chapter_output"])

    worker_count = max(1, min(merge_workers, len(manifest)))
    if worker_count == 1:
        for item in manifest:
            output_path = merge_one(item)
            print(f"Merged chapter: {output_path}")
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(merge_one, item) for item in manifest]
            for future in futures:
                print(f"Merged chapter: {future.result()}")
    merge_audio(
        book_concat_path,
        final_book_path,
        audio_metadata=build_audio_metadata(
            epub_metadata,
            title=render_metadata.get("selection_title", epub_metadata.title),
            album=render_metadata.get("title", epub_metadata.title),
        ),
        cover_path=cover_path,
    )
    print(f"Merged final audiobook: {final_book_path}")


def main() -> int:
    args = parse_args()
    render_dir = Path(args.render_dir).expanduser().resolve() if args.render_dir else None
    epub_path = Path(args.epub).expanduser().resolve() if args.epub else None
    if render_dir and epub_path:
        raise SystemExit("Use either an EPUB path or --render-dir, not both.")
    if not render_dir and epub_path is None:
        raise SystemExit("Provide an EPUB path or --render-dir.")
    if render_dir and args.prepare_only:
        raise SystemExit("--prepare-only cannot be used with --render-dir.")
    if epub_path is not None and not epub_path.exists():
        raise SystemExit(f"EPUB not found: {epub_path}")

    out_root = Path(args.out_root).expanduser().resolve()
    secret_file = Path(args.secret_file).expanduser().resolve()
    skip_titles = set(DEFAULT_SKIP_TITLES)
    skip_titles.update(args.skip_title)

    if render_dir:
        jobs_path = render_dir / "jobs.jsonl"
        manifest_path = render_dir / "manifest.json"
        metadata_path = render_dir / "metadata.json"
        book_concat_path = render_dir / "merge" / "book.txt"
        if not jobs_path.exists() or not manifest_path.exists() or not metadata_path.exists():
            raise SystemExit(
                f"Render dir must contain jobs.jsonl, manifest.json, and metadata.json: {render_dir}"
            )
        render_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        audio_format = args.audio_format or render_metadata.get("audio_format", DEFAULT_AUDIO_FORMAT)
        speed = args.speed if args.speed != 1.0 else float(render_metadata.get("speed", 1.0))
        voice = args.voice if args.voice != DEFAULT_TTS_VOICE else str(render_metadata.get("voice", DEFAULT_TTS_VOICE))
        tts_model = args.tts_model if args.tts_model != DEFAULT_TTS_MODEL else str(
            render_metadata.get("tts_model", DEFAULT_TTS_MODEL)
        )
        instructions = args.instructions or default_instructions(speed)
        final_name = render_metadata.get("book_slug", render_dir.name)
        selection_slug = render_metadata.get("selection_slug", "selection")
        if selection_slug != "full-book":
            final_name = f"{final_name}-{selection_slug}"
        final_book_path = render_dir / f"{final_name}.{audio_format}"
        load_api_key(secret_file)
        run_tts_batch(
            jobs_path=jobs_path,
            out_dir=render_dir / "parts",
            tts_model=tts_model,
            voice=voice,
            speed=speed,
            audio_format=audio_format,
            instructions=instructions,
            rpm=args.rpm,
            tts_workers=args.tts_workers,
            tts_attempts=args.tts_attempts,
            tts_shard_retries=args.tts_shard_retries,
            force=args.force,
        )
        merge_all(
            manifest_path=manifest_path,
            metadata_path=metadata_path,
            book_concat_path=book_concat_path,
            final_book_path=final_book_path,
            merge_workers=args.merge_workers,
        )
        return 0

    metadata, chapters, _raw_texts = parse_epub(epub_path, skip_titles)
    if args.list_chapters:
        if metadata.title:
            print(f"Title: {metadata.title}")
        if metadata.creator:
            print(f"Author: {metadata.creator}")
        for idx, chapter in enumerate(chapters, start=1):
            print(f"{idx:02d}. {chapter.title}")
        return 0

    selected_clean_backend = choose_clean_backend(args.clean_backend, not args.no_clean)
    needs_api_key = (selected_clean_backend == "api") or (not args.prepare_only)
    if needs_api_key:
        load_api_key(secret_file)

    instructions = args.instructions or default_instructions(args.speed)
    prepared = prepare_outputs(
        epub_path=epub_path,
        out_root=out_root,
        voice=args.voice,
        speed=args.speed,
        audio_format=args.audio_format,
        tts_model=args.tts_model,
        clean_model=args.clean_model,
        instructions=instructions,
        max_chars=args.max_chars,
        cleanup_max_chars=args.cleanup_max_chars,
        skip_titles=skip_titles,
        use_clean_model=not args.no_clean,
        clean_backend=selected_clean_backend,
        cleanup_workers=args.cleanup_workers,
        chapter_numbers=args.chapter_number,
        chapter_matches=args.chapter_match,
    )

    if args.prepare_only:
        return 0

    base_dir = Path(prepared["base_dir"])
    run_tts_batch(
        jobs_path=Path(prepared["jobs_path"]),
        out_dir=base_dir / "parts",
        tts_model=args.tts_model,
        voice=args.voice,
        speed=args.speed,
        audio_format=args.audio_format,
        instructions=instructions,
        rpm=args.rpm,
        tts_workers=args.tts_workers,
        tts_attempts=args.tts_attempts,
        tts_shard_retries=args.tts_shard_retries,
        force=args.force,
    )
    merge_all(
        manifest_path=Path(prepared["manifest_path"]),
        metadata_path=Path(prepared["metadata_path"]),
        book_concat_path=Path(prepared["book_concat_path"]),
        final_book_path=Path(prepared["final_book_path"]),
        merge_workers=args.merge_workers,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
