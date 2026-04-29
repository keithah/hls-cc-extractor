# HLS CC Extractor

Extract embedded CEA-608 closed captions from HLS live streams without shelling out to `ffmpeg`.

This is useful for HLS streams that advertise `TYPE=CLOSED-CAPTIONS` in the master manifest but do not provide a separate WebVTT subtitle playlist. In that setup, captions are carried as A53/CEA-608 side data inside the video stream.

## Install

Requires Python 3.9+ and PyAV:

```bash
python3 -m pip install -r requirements.txt
```

## Usage

```bash
python3 extract_hls_cc.py \
  "https://example.com/live/master.m3u8" \
  --seconds 300 \
  --output captions.srt
```

The script:

1. Fetches the HLS master manifest.
2. Selects the highest-bandwidth media playlist.
3. Polls live MPEG-TS segments.
4. Uses PyAV to decode video frames.
5. Reads A53 closed-caption side data from decoded frames.
6. Parses CEA-608 field 1 text and writes SRT cues.

## Example

```bash
python3 extract_hls_cc.py \
  "https://nbcsradio-tni-drct-quwkz.fast.nbcuni.com/live/master.m3u8" \
  -t 300 \
  -o captions.srt
```

While running, status is printed to stderr:

```text
Manifest declares CLOSED-CAPTIONS.
Using media playlist: https://...
Wrote 123 caption cues to captions.srt
```

## Notes

This is a compact extractor, not a full broadcast-caption renderer. It focuses on recovering readable text from CEA-608 field 1 captions. Styling, row positioning, colors, roll-up behavior, and some special characters are intentionally simplified.

If the output file is empty, the stream may still contain a caption channel. Live streams often carry empty or control-only caption packets for periods of time. Run a longer capture before assuming captions are unavailable.

## License

MIT
