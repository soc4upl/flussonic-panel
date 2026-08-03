from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any
from urllib.parse import urlsplit

_EXTINF_NAME_RE = re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE)
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")

_CYRILLIC_MAP = str.maketrans({
    "А":"A","Б":"B","В":"V","Г":"G","Д":"D","Е":"E","Ё":"Yo","Ж":"Zh","З":"Z","И":"I","Й":"Y","К":"K","Л":"L","М":"M","Н":"N","О":"O","П":"P","Р":"R","С":"S","Т":"T","У":"U","Ф":"F","Х":"Kh","Ц":"Ts","Ч":"Ch","Ш":"Sh","Щ":"Sch","Ъ":"","Ы":"Y","Ь":"","Э":"E","Ю":"Yu","Я":"Ya",
    "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"yo","ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m","н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u","ф":"f","х":"kh","ц":"ts","ч":"ch","ш":"sh","щ":"sch","ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya",
    "І":"I","і":"i","Ї":"Yi","ї":"yi","Є":"Ye","є":"ye","Ґ":"G","ґ":"g"
})


def _display_name(extinf: str) -> str:
    # The text after the final comma is the playlist-visible channel name.
    if "," in extinf:
        candidate = extinf.rsplit(",", 1)[1].strip()
        if candidate:
            return candidate
    match = _EXTINF_NAME_RE.search(extinf)
    return match.group(1).strip() if match else ""


def stream_name_from_title(title: str, url: str) -> str:
    normalized = unicodedata.normalize("NFKD", title.translate(_CYRILLIC_MAP))
    ascii_title = normalized.encode("ascii", "ignore").decode("ascii")
    value = _SAFE_NAME_RE.sub("_", ascii_title.strip()).strip("_.-")
    value = re.sub(r"_+", "_", value)
    if not value:
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
        value = f"channel_{digest}"
    return value[:255]


def parse_m3u(text: str) -> dict[str, Any]:
    lines = [line.strip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    items: list[dict[str, str]] = []
    warnings: list[str] = []
    seen_pairs: set[tuple[str, str]] = set()
    used_names: dict[str, int] = {}
    pending_extinf: str | None = None

    for line_no, line in enumerate(lines, start=1):
        if not line:
            continue
        if line.upper().startswith("#EXTINF:"):
            if pending_extinf is not None:
                warnings.append(f"Строка {line_no}: предыдущий #EXTINF не содержит URL")
            pending_extinf = line
            continue
        if line.startswith("#"):
            continue
        if pending_extinf is None:
            warnings.append(f"Строка {line_no}: URL без #EXTINF пропущен")
            continue

        title = _display_name(pending_extinf)
        url = line
        pending_extinf = None
        if not title:
            warnings.append(f"Строка {line_no}: не найдено название канала")
            continue
        if any(char in url for char in ("\n", "\r", "\x00")) or len(url) > 4096:
            warnings.append(f"{title}: некорректный URL")
            continue
        parts = urlsplit(url)
        if not parts.scheme:
            warnings.append(f"{title}: URL без протокола пропущен")
            continue

        pair = (title, url)
        if pair in seen_pairs:
            warnings.append(f"{title}: повтор в M3U пропущен")
            continue
        seen_pairs.add(pair)

        base_name = stream_name_from_title(title, url)
        count = used_names.get(base_name, 0) + 1
        used_names[base_name] = count
        system_name = base_name if count == 1 else f"{base_name[:245]}_{count}"
        items.append({"name": system_name, "title": title, "url": url})

    if pending_extinf is not None:
        warnings.append("Последний #EXTINF не содержит URL")

    return {"items": items, "warnings": warnings, "count": len(items)}
