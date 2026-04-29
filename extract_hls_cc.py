#!/usr/bin/env python3
"""
Extract embedded CEA-608 captions from an HLS stream without invoking ffmpeg.

Dependency:
    python3 -m pip install av

Example:
    python3 extract_hls_cc.py \
      https://nbcsradio-tni-drct-quwkz.fast.nbcuni.com/live/master.m3u8 \
      -t 300 -o captions.srt
"""

from __future__ import annotations

import argparse
import ctypes
import io
import sys
import time
from dataclasses import dataclass
from fractions import Fraction
from typing import Iterable
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import av


USER_AGENT = "caption-extractor/1.0"


def fetch_text(url: str, timeout: float = 15.0) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_bytes(url: str, timeout: float = 20.0) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as response:
        return response.read()


@dataclass
class MediaSegment:
    url: str
    duration: float


def parse_master(manifest_url: str, manifest: str) -> tuple[str, bool]:
    """Return the media playlist URL and whether captions are declared."""
    has_closed_captions = "TYPE=CLOSED-CAPTIONS" in manifest
    lines = [line.strip() for line in manifest.splitlines() if line.strip()]

    lowest_bandwidth = None
    lowest_url = None
    pending_bandwidth = None
    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            pending_bandwidth = -1
            attrs = line.split(":", 1)[1].split(",")
            for attr in attrs:
                if attr.startswith("BANDWIDTH="):
                    try:
                        pending_bandwidth = int(attr.split("=", 1)[1])
                    except ValueError:
                        pending_bandwidth = -1
        elif pending_bandwidth is not None and not line.startswith("#"):
            if lowest_bandwidth is None or pending_bandwidth < lowest_bandwidth:
                lowest_bandwidth = pending_bandwidth
                lowest_url = urljoin(manifest_url, line)
            pending_bandwidth = None

    return lowest_url or manifest_url, has_closed_captions


def parse_media_playlist(playlist_url: str, manifest: str) -> list[MediaSegment]:
    segments: list[MediaSegment] = []
    pending_duration = 0.0
    for raw_line in manifest.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            value = line.split(":", 1)[1].split(",", 1)[0]
            try:
                pending_duration = float(value)
            except ValueError:
                pending_duration = 0.0
        elif not line.startswith("#"):
            segments.append(MediaSegment(urljoin(playlist_url, line), pending_duration))
            pending_duration = 0.0
    return segments


def side_data_bytes(side_data) -> bytes:
    return ctypes.string_at(side_data.buffer_ptr, side_data.buffer_size)


def iter_a53_cc_packets(segment: bytes) -> Iterable[tuple[float, bytes]]:
    """Yield (seconds, a53_cc_payload) from decoded video frame side data."""
    container = av.open(io.BytesIO(segment), format="mpegts")
    video_stream = next((stream for stream in container.streams if stream.type == "video"), None)
    if video_stream is None:
        return

    for packet in container.demux(video_stream):
        for frame in packet.decode():
            if frame.pts is None:
                timestamp = 0.0
            else:
                time_base: Fraction = frame.time_base or video_stream.time_base
                timestamp = float(frame.pts * time_base)

            for side_data in frame.side_data:
                if "A53_CC" in str(side_data.type):
                    yield timestamp, side_data_bytes(side_data)


def parity_stripped(byte: int) -> int:
    return byte & 0x7F


class Cea608Decoder:
    """Small CEA-608 field-1 decoder focused on plain text extraction."""

    def __init__(self) -> None:
        self.buffer: list[str] = []
        self.last_control: tuple[int, int] | None = None

    def accept_a53(self, payload: bytes) -> list[str]:
        emitted: list[str] = []

        for offset in range(0, len(payload) - 2, 3):
            cc_header = payload[offset]
            cc_valid = bool(cc_header & 0x04)
            cc_type = cc_header & 0x03
            if not cc_valid or cc_type != 0:
                continue

            c1 = parity_stripped(payload[offset + 1])
            c2 = parity_stripped(payload[offset + 2])
            if c1 == 0 and c2 == 0:
                continue

            text = self._accept_pair(c1, c2)
            if text:
                emitted.append(text)

        return emitted

    def flush(self) -> str | None:
        text = self._clean("".join(self.buffer))
        self.buffer.clear()
        return text or None

    def _accept_pair(self, c1: int, c2: int) -> str | None:
        # Most CEA-608 control codes are repeated. Ignore the duplicate.
        is_control = 0x10 <= c1 <= 0x1F
        if is_control:
            pair = (c1, c2)
            if pair == self.last_control:
                return None
            self.last_control = pair
        else:
            self.last_control = None

        if c1 in (0x14, 0x1C):
            if c2 == 0x21:  # backspace
                if self.buffer:
                    self.buffer.pop()
                return None
            if c2 in (0x2C, 0x2D, 0x2F):  # EDM, CR, EOC
                return self.flush()
            return None

        if is_control:
            return None

        for char_code in (c1, c2):
            if 0x20 <= char_code <= 0x7E:
                self.buffer.append(chr(char_code))
        return None

    @staticmethod
    def _clean(text: str) -> str:
        return " ".join(text.replace("\r", " ").replace("\n", " ").split())


def srt_time(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    millis = round(seconds * 1000)
    hours, rem = divmod(millis, 3_600_000)
    minutes, rem = divmod(rem, 60_000)
    secs, millis = divmod(rem, 1000)
    return f"{hours:02}:{minutes:02}:{secs:02},{millis:03}"


def write_srt_cue(output, index: int, start: float, end: float, text: str) -> None:
    output.write(f"{index}\n{srt_time(start)} --> {srt_time(end)}\n{text}\n\n")
    output.flush()


def extract(hls_url: str, output_path: str, seconds: float, poll_interval: float) -> int:
    master = fetch_text(hls_url)
    media_url, has_declared_cc = parse_master(hls_url, master)
    if has_declared_cc:
        print("Manifest declares CLOSED-CAPTIONS.", file=sys.stderr)
    else:
        print("Manifest does not declare CLOSED-CAPTIONS; still checking video side data.", file=sys.stderr)

    print(f"Using media playlist: {media_url}", file=sys.stderr)

    seen_segments: set[str] = set()
    decoder = Cea608Decoder()
    cue_index = 1
    started = time.monotonic()
    first_pts: float | None = None
    last_emit_at = 0.0

    with open(output_path, "w", encoding="utf-8") as output:
        while time.monotonic() - started < seconds:
            playlist = fetch_text(media_url)
            segments = parse_media_playlist(media_url, playlist)

            new_segments = [segment for segment in segments if segment.url not in seen_segments]
            if not new_segments:
                time.sleep(poll_interval)
                continue

            for segment in new_segments:
                seen_segments.add(segment.url)
                try:
                    payload = fetch_bytes(segment.url)
                except Exception as exc:
                    print(f"Skipping segment fetch failure: {segment.url}: {exc}", file=sys.stderr)
                    continue

                for pts, a53_payload in iter_a53_cc_packets(payload):
                    if first_pts is None:
                        first_pts = pts
                    rel_time = pts - first_pts
                    for text in decoder.accept_a53(a53_payload):
                        end = max(rel_time, last_emit_at + 1.0)
                        start = max(last_emit_at, end - max(1.5, min(5.0, len(text) / 12.0)))
                        write_srt_cue(output, cue_index, start, end, text)
                        cue_index += 1
                        last_emit_at = end

            time.sleep(poll_interval)

        tail = decoder.flush()
        if tail:
            end = max(last_emit_at + 2.0, seconds)
            write_srt_cue(output, cue_index, last_emit_at, end, tail)
            cue_index += 1

    return cue_index - 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract embedded CEA-608 captions from HLS to SRT.")
    parser.add_argument("url", help="HLS master or media playlist URL")
    parser.add_argument("-o", "--output", default="captions.srt", help="Output SRT path")
    parser.add_argument("-t", "--seconds", type=float, default=300.0, help="How long to watch the live stream")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Seconds between playlist polls")
    args = parser.parse_args()

    count = extract(args.url, args.output, args.seconds, args.poll_interval)
    print(f"Wrote {count} caption cues to {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
