from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path


NAME_PATTERN = r"(?=[A-Z0-9-]*[A-Z])[A-Z0-9]+(?:-[A-Z0-9]+)*"
GROUP_RE = re.compile(rf"01\s+({NAME_PATTERN})\.", re.IGNORECASE)
FIELD_RE = re.compile(rf"05\s+({NAME_PATTERN})\s+PIC\s+(.+?)\.", re.IGNORECASE)
DISPLAY_ALPHA_RE = re.compile(r"X\(([0-9]+)\)", re.IGNORECASE)
DISPLAY_NUMERIC_RE = re.compile(r"9\(([0-9]+)\)", re.IGNORECASE)
PACKED_RE = re.compile(r"S?9\(([0-9]+)\)(?:V9\(([0-9]+)\)|(V99))?\s+COMP-3", re.IGNORECASE)


@dataclass(frozen=True)
class FieldLayout:
    name: str
    picture: str
    kind: str
    offset: int
    length: int
    digits: int = 0
    scale: int = 0


def _picture_layout(picture: str) -> tuple[str, int, int, int]:
    alpha = DISPLAY_ALPHA_RE.fullmatch(picture)
    numeric = DISPLAY_NUMERIC_RE.fullmatch(picture)
    if alpha or numeric:
        length = int((alpha or numeric).group(1))
        if length > 0:
            return ("text", length, 0, 0) if alpha else ("zoned", length, length, 0)

    packed = PACKED_RE.fullmatch(picture)
    if packed:
        whole_digits = int(packed.group(1))
        scale = 2 if packed.group(3) else int(packed.group(2) or 0)
        if whole_digits > 0 and (packed.group(2) is None or scale > 0):
            digits = whole_digits + scale
            return "packed_decimal", (digits + 2) // 2, digits, scale
    raise ValueError(f"Unsupported or malformed copybook PIC clause: {picture}")


def parse_copybook(path: str | Path) -> list[FieldLayout]:
    """Read one 01 group with flat 05 fields, each on one line ending in a period.

    PIC supports X(n), unsigned 9(n), and [S]9(n)[V99|V9(n)] COMP-3.
    Only blank lines, full-line *> comments and '*' in column 7 (preceded
    by six spaces) are ignored. Clauses, nested groups, continuation lines,
    sequence columns, FILLER, REDEFINES and OCCURS are not supported.
    """
    offset = 0
    fields: list[FieldLayout] = []
    names: set[str] = set()
    group_seen = False

    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("*>") or line.startswith("      *"):
            continue
        line = line.strip()
        group = GROUP_RE.fullmatch(line)
        if group and not group_seen and group.group(1).upper() != "FILLER":
            group_seen = True
            continue
        match = FIELD_RE.fullmatch(line)
        if not group_seen or not match:
            raise ValueError(f"Unsupported copybook line {line_number}: {line}")
        raw_name, picture = match.groups()
        name = raw_name.lower().replace("-", "_")
        picture = picture.upper().strip()
        if name == "filler" or name in names:
            raise ValueError(f"Unsupported or duplicate field name on line {line_number}: {raw_name}")
        kind, length, digits, scale = _picture_layout(picture)
        fields.append(FieldLayout(name, picture, kind, offset, length, digits, scale))
        names.add(name)
        offset += length

    if not fields:
        raise ValueError(f"No supported fields found in copybook: {path}")

    return fields


def record_length(fields: list[FieldLayout]) -> int:
    """Validate a contiguous, unambiguous layout before using its byte offsets."""
    if not fields:
        raise ValueError("Record layout must contain fields")
    offset = 0
    names: set[str] = set()
    for field in fields:
        if not isinstance(field, FieldLayout):
            raise ValueError("Record layout must contain FieldLayout values")
        if (
            not isinstance(field.name, str)
            or not re.fullmatch(r"[a-z0-9]+(?:_[a-z0-9]+)*", field.name)
            or not re.search(r"[a-z]", field.name)
            or field.name == "filler"
            or field.name in names
        ):
            raise ValueError(f"Invalid or duplicate field name: {field.name}")
        if any(type(value) is not int for value in (field.offset, field.length, field.digits, field.scale)):
            raise ValueError(f"Layout dimensions must be integers: {field.name}")
        if field.offset != offset:
            raise ValueError(f"Noncontiguous field offset: {field.name}")
        if not isinstance(field.picture, str) or (
            field.kind, field.length, field.digits, field.scale
        ) != _picture_layout(field.picture):
            raise ValueError(f"Layout does not match PIC clause: {field.name}")
        names.add(field.name)
        offset += field.length
    return offset


def parse_record(record: bytes, fields: list[FieldLayout], *, code_page: str = "cp037") -> dict[str, object]:
    """Decode cp037 text, F0-F9 unsigned zoned integers and exact decimal strings."""
    if not isinstance(code_page, str) or code_page.lower() != "cp037":
        raise ValueError(f"Unsupported code page: {code_page}; only cp037 is supported")
    if not isinstance(record, (bytes, bytearray)):
        raise ValueError("Record must be raw bytes")
    expected = record_length(fields)
    if len(record) != expected:
        raise ValueError(f"Record has {len(record)} bytes; expected {expected}")

    parsed: dict[str, object] = {}
    for field in fields:
        raw = record[field.offset : field.offset + field.length]
        if field.kind == "text":
            parsed[field.name] = raw.decode("cp037").strip()
        elif field.kind == "zoned":
            if any(byte < 0xF0 or byte > 0xF9 for byte in raw):
                raise ValueError(f"Invalid unsigned zoned decimal bytes: {field.name}")
            parsed[field.name] = int(raw.decode("cp037"))
        elif field.kind == "packed_decimal":
            value = unpack_comp3(raw, field.scale, digits=field.digits)
            if not field.picture.upper().startswith("S") and value.is_signed():
                raise ValueError(f"Negative sign in unsigned packed field: {field.name}")
            parsed[field.name] = format(value, "f")
        else:
            raise ValueError(f"Unsupported field kind: {field.kind}")

    return parsed


def _validate_precision(digits: int, scale: int) -> None:
    if type(digits) is not int or digits <= 0:
        raise ValueError("Packed decimal digits must be a positive integer")
    if type(scale) is not int or not 0 <= scale <= digits:
        raise ValueError("Packed decimal scale must be an integer between zero and digits")


def unpack_comp3(raw: bytes, scale: int, *, digits: int | None = None) -> Decimal:
    """Accept C/F positive or D negative signs only, with decimal digit nibbles.

    Pass declared digits to validate byte length and the zero high-nibble pad
    required for even precision. Without digits, all nibbles before the sign
    are treated as digits; the declared precision cannot be inferred.
    """
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        raise ValueError("COMP-3 value must contain raw bytes")
    declared_digits = digits if digits is not None else len(raw) * 2 - 1
    _validate_precision(declared_digits, scale)
    if len(raw) != (declared_digits + 2) // 2:
        raise ValueError(f"COMP-3 byte length does not match precision {declared_digits}")
    nibbles: list[int] = []
    for byte in raw:
        nibbles.append(byte >> 4)
        nibbles.append(byte & 0x0F)

    sign_nibble = nibbles.pop()
    if sign_nibble not in (0x0C, 0x0D, 0x0F):
        raise ValueError(f"Invalid COMP-3 sign nibble: {sign_nibble:X}")

    if any(nibble > 9 for nibble in nibbles):
        raise ValueError("Invalid COMP-3 digit nibble")
    if declared_digits % 2 == 0:
        if nibbles.pop(0) != 0:
            raise ValueError("COMP-3 leading pad nibble must be zero")
    return Decimal((int(sign_nibble == 0x0D), tuple(nibbles), -scale))


def pack_comp3(value: str | Decimal, digits: int, scale: int) -> bytes:
    """Pack exactly, independent of decimal context; never round or truncate."""
    _validate_precision(digits, scale)
    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid packed decimal value: {value}") from exc
    if not decimal_value.is_finite():
        raise ValueError("Packed decimal value must be finite")
    sign_bit, coefficient, exponent = decimal_value.as_tuple()
    sign = "D" if sign_bit else "C"
    digit_text = "".join(str(digit) for digit in coefficient)
    shift = exponent + scale
    if decimal_value.is_zero():
        digit_text = "0"
    elif shift < 0:
        remove = -shift
        if remove >= len(digit_text) or any(digit != "0" for digit in digit_text[-remove:]):
            raise ValueError(f"Value {value} loses fractional precision at scale {scale}")
        digit_text = digit_text[:-remove]
    elif len(digit_text) + shift > digits:
        raise ValueError(f"Value {value} exceeds packed decimal precision {digits}")
    else:
        digit_text += "0" * shift
    if len(digit_text) > digits:
        raise ValueError(f"Value {value} exceeds packed decimal precision {digits}")
    digit_text = digit_text.zfill(digits)

    nibbles = digit_text + sign
    if len(nibbles) % 2:
        nibbles = "0" + nibbles

    return bytes(int(nibbles[index : index + 2], 16) for index in range(0, len(nibbles), 2))
