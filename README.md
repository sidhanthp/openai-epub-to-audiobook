# OpenAI EPUB to Audiobook

Turn an `EPUB` into a single audiobook file with:

- EPUB chapter parsing
- optional text cleanup before speech
- OpenAI TTS batch rendering
- final merge with audiobook-friendly metadata and cover art

This repo intentionally excludes any books, generated audio, or secrets.

## What It Solves

- Handles EPUBs whose package files live under nested paths such as `OEBPS/content.opf`
- Keeps cleanup non-destructive by stripping repeated headers only at the start of extracted text
- Isolates one-chapter samples from full-book renders
- Lists chapters without requiring API credentials
- Supports resume/render-only mode from an existing prepared directory

## Requirements

- Python `3.10+`
- `ffmpeg` and `ffprobe`
- Optional: `codex` CLI if you want Codex-based cleanup
- `OPENAI_API_KEY`

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

Set `OPENAI_API_KEY` in your shell or `.env` file before live TTS runs.

## Quick Start

List chapters without touching the API:

```bash
epub-to-audiobook /path/to/book.epub --list-chapters
```

Prepare one chapter only:

```bash
epub-to-audiobook /path/to/book.epub \
  --chapter-number 5 \
  --prepare-only
```

Run a one-chapter sample:

```bash
epub-to-audiobook /path/to/book.epub \
  --chapter-number 5 \
  --voice marin \
  --speed 1.5 \
  --audio-format mp3
```

Run a full book:

```bash
epub-to-audiobook /path/to/book.epub \
  --voice marin \
  --speed 1.5 \
  --audio-format mp3 \
  --cleanup-workers 4 \
  --tts-workers 2 \
  --rpm 20
```

Resume from a prepared render directory:

```bash
epub-to-audiobook \
  --render-dir output/library/my-book/full-book \
  --tts-workers 2 \
  --rpm 20
```

## Output Layout

Each run creates a selection-specific directory:

```text
output/library/<book-slug>/<selection-slug>/
```

Inside it:

- `raw_text/`: extracted text after local cleanup
- `clean_text/`: cleanup output used for TTS
- `parts/`: per-chunk audio files
- `merge/`: concat manifests
- `assets/`: extracted cover image if present
- `jobs.jsonl`: prepared TTS jobs
- `manifest.json`: chapter manifest
- `metadata.json`: source and render metadata
- `<final>.mp3|flac|wav`: merged audiobook

## Cleanup Backends

- `auto`: prefer Codex CLI if available, otherwise OpenAI API
- `codex`: use Codex CLI for cleanup
- `api`: use a cheap OpenAI text model for cleanup
- `local`: skip model cleanup and use only deterministic local cleanup

Codex cleanup is bounded with retries and a timeout. If it fails on a chunk, the script falls back to the locally cleaned text instead of aborting the whole run.

## Notes

- This tool does not grant rights to transform copyrighted works. Only use it with content you are allowed to process.
- If you hit quota exhaustion on the Audio API, the prepared directory remains reusable. Recharge quota and rerun with `--render-dir`.
