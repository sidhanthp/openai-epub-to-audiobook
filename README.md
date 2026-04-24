# OpenAI EPUB to Audiobook

Turn an `EPUB` into a single audiobook file with:

- EPUB chapter parsing
- optional text cleanup before speech
- OpenAI TTS rendering with cheap-by-default batch mode
- final `m4b` merge with audiobook-friendly metadata, cover art, and chapters

This repo intentionally excludes any books, generated audio, or secrets.

## What It Solves

- Handles EPUBs whose package files live under nested paths such as `OEBPS/content.opf`
- Keeps cleanup non-destructive by stripping repeated headers only at the start of extracted text
- Isolates one-chapter samples from full-book renders
- Lists chapters without requiring API credentials
- Supports resume/render-only mode from an existing prepared directory
- Transfers EPUB title, author, publisher, date, language, identifier, description, cover art, and chapter titles when available

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

Set `OPENAI_API_KEY` in your shell or `.env` file before TTS runs.

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

Run a one-chapter sample fast:

```bash
epub-to-audiobook /path/to/book.epub \
  --chapter-number 5 \
  --rush \
  --voice marin \
  --speed 1.5 \
  --audio-format mp3 \
  --final-format m4b
```

Submit a full book in default cheap batch mode:

```bash
epub-to-audiobook /path/to/book.epub \
  --voice marin \
  --speed 1.5 \
  --audio-format mp3 \
  --final-format m4b \
  --cleanup-workers 4
```

Submit a full book and wait for the batch to finish:

```bash
epub-to-audiobook /path/to/book.epub \
  --voice marin \
  --speed 1.5 \
  --audio-format mp3 \
  --final-format m4b \
  --cleanup-workers 4 \
  --wait
```

Resume from a prepared render directory and wait for batch completion:

```bash
epub-to-audiobook \
  --render-dir output/library/my-book/full-book \
  --wait
```

Resume from a prepared render directory in fast live mode:

```bash
epub-to-audiobook \
  --render-dir output/library/my-book/full-book \
  --rush \
  --tts-workers 2 \
  --rpm 20
```

## TTS Modes

- Default: batch. Cheapest path for full-book final renders.
- `--wait`: stay attached to the batch job, poll until it completes, materialize outputs, and merge the final audiobook.
- `--rush`: switch to live TTS for faster interactive samples and retakes.
- `--tts-backend`: advanced override if you want to force `live` or `batch` directly.

`--audio-format` controls the OpenAI TTS chunk format. `--final-format` controls the final audiobook file. The default is MP3 chunks plus an `m4b` final because `m4b` is the most reliable single-file format for audiobook chapters in common players.

Recommended workflow:

- Samples and retakes: `--rush`
- Full-book finals: default batch, usually with `--wait`
- Long-running background jobs: submit in default batch mode, then later resume with `--render-dir ... --wait`

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
- `<final>.m4b|mp3|flac|wav`: merged audiobook

## Audiobook Metadata

When the EPUB provides the fields, the final audiobook carries over:

- title and selection title
- author as artist, album artist, and composer
- publisher, date/year, language, and identifier/ISBN
- description/comment
- cover image
- chapter titles from the EPUB table of contents

The default `m4b` final output also embeds a chapter table using the merged chapter durations, so audiobook players can show chapter navigation inside the single final file. MP3, FLAC, and WAV final files keep normal tags, but chapter navigation support is player-dependent and less reliable than `m4b`.

## Cleanup Backends

- `auto`: prefer Codex CLI if available, otherwise OpenAI API
- `codex`: use Codex CLI for cleanup
- `api`: use a cheap OpenAI text model for cleanup
- `local`: skip model cleanup and use only deterministic local cleanup

Codex cleanup is bounded with retries and a timeout. If it fails on a chunk, the script falls back to the locally cleaned text instead of aborting the whole run.

## Batch Resume Behavior

When you run in default batch mode without `--wait`, the tool:

- prepares text and jobs
- submits the batch
- exits after printing the exact `--render-dir ... --wait` command shape to resume later

If a batch is already active, rerunning with `--render-dir` prints the current batch status. Add `--wait` to keep polling until completion.

## Notes

- This tool does not grant rights to transform copyrighted works. Only use it with content you are allowed to process.
- If you hit quota exhaustion on the Audio API, the prepared directory remains reusable. Recharge quota and rerun with `--render-dir`.
