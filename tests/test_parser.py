from dataclasses import replace
from decimal import Decimal, Inexact, localcontext

import pytest

from vsam_offload.copybook import FieldLayout, pack_comp3, parse_copybook, parse_record, record_length, unpack_comp3
from vsam_offload.generate_sample import SAMPLE_ROWS, build_record


def test_copybook_layout_record_length():
    fields = parse_copybook("samples/copybooks/ACCOUNT_BALANCE.cbl")
    assert record_length(fields) == 51
    assert [field.name for field in fields] == [
        "account_id",
        "branch",
        "current_balance",
        "available_limit",
        "last_txn_date",
        "sequence_number",
        "status",
    ]


def test_comp3_roundtrip_negative_value():
    packed = pack_comp3("-15.10", digits=15, scale=2)
    assert unpack_comp3(packed, scale=2) == Decimal("-15.10")


def test_parse_generated_record():
    fields = parse_copybook("samples/copybooks/ACCOUNT_BALANCE.cbl")
    record = build_record(
        {
            "account_id": "000000100001",
            "branch": 1601,
            "current_balance": "12450.70",
            "available_limit": "8000.00",
            "last_txn_date": 20260923,
            "sequence_number": 1000000001,
            "status": "A",
        }
    )
    parsed = parse_record(record, fields)
    assert parsed["account_id"] == "000000100001"
    assert parsed["branch"] == 1601
    assert parsed["current_balance"] == "12450.70"
    assert parsed["sequence_number"] == 1000000001


def write_copybook(tmp_path, source):
    path = tmp_path / "layout.cbl"
    path.write_text(source, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "picture,digits,scale,length",
    [
        ("S9(13)V99 COMP-3", 15, 2, 8),
        ("S9(2)V9(3) COMP-3", 5, 3, 3),
        ("9(1)V9(10) COMP-3", 11, 10, 6),
        ("S9(2)V9(9) COMP-3", 11, 9, 6),
        ("9(4) COMP-3", 4, 0, 3),
        ("S9(1) COMP-3", 1, 0, 1),
    ],
)
def test_supported_packed_pictures(tmp_path, picture, digits, scale, length):
    path = write_copybook(tmp_path, f"01 RECORD.\n05 AMOUNT PIC {picture}.\n")
    field, = parse_copybook(path)
    assert (field.digits, field.scale, field.length) == (digits, scale, length)
    raw = pack_comp3(Decimal((0, (1,), -scale)), digits, scale)
    assert parse_record(raw, [field]) == {"amount": format(Decimal((0, (1,), -scale)), "f")}


def test_explicit_comment_and_blank_subset(tmp_path):
    source = (
        "\n*> free-form comment\n      * fixed-format comment\n"
        "       01 record.\n\n   *> another comment\n"
        "           05 key-name pic x(2).\n05 count pic 9(2).\n"
    )
    fields = parse_copybook(write_copybook(tmp_path, source))
    assert parse_record("AB09".encode("cp037"), fields) == {"key_name": "AB", "count": 9}


@pytest.mark.parametrize(
    "line",
    [
        "05 ALIAS REDEFINES BASE PIC X(1).",
        "05 BASE PIC X(1) REDEFINES OTHER.",
        "05 ITEMS OCCURS 2 TIMES PIC X(1).",
        "05 ITEMS PIC X(1) OCCURS 2 TIMES.",
        "05 NESTED.",
        "10 CHILD PIC X(1).",
        "77 STANDALONE PIC X(1).",
        "88 CONDITION VALUE 'A'.",
        "66 ALIAS RENAMES BASE.",
        "01 SECOND.",
        "01 SECOND PIC X(1).",
        "COPY SOMETHING.",
        "garbage",
        "05 BASE PIC X(1)",
        "05 BASE PIC X(1). *> inline comment",
        "000100 05 BASE PIC X(1).",
        "      - PIC X(1).",
        "      / page comment",
        "* not a column-seven comment",
        "05 FILLER PIC X(1).",
        "05 BASE PIC X.",
        "05 BASE PIC X(0).",
        "05 BASE PIC X(-1).",
        "05 BASE PIC X(1.5).",
        "05 BASE PIC X(1) VALUE 'A'.",
        "05 BASE PIC 9(0).",
        "05 BASE PIC S9(2).",
        "05 BASE PIC 9(2)V99.",
        "05 BASE PIC 9(2) COMP.",
        "05 BASE PIC S9(0)V99 COMP-3.",
        "05 BASE PIC S9(1)V9(0) COMP-3.",
        "05 BASE PIC S9(1)V9(2)9 COMP-3.",
        "05 BASE PIC S9(1)V999 COMP-3.",
        "05 BASE PIC X(１).",
        "05 BAD_NAME PIC X(1).",
        "05 123 PIC X(1).",
    ],
)
def test_unsupported_copybook_line_rejected(tmp_path, line):
    # Include a valid field so skipping the bad line would incorrectly succeed.
    source = f"01 RECORD.\n05 GOOD PIC X(1).\n{line}\n"
    with pytest.raises(ValueError):
        parse_copybook(write_copybook(tmp_path, source))


@pytest.mark.parametrize(
    "source",
    [
        "",
        "\n*> comment only\n",
        "01 EMPTY.",
        "05 NO-GROUP PIC X(1).",
        "01 GROUP OCCURS 2 TIMES.\n05 ITEM PIC X(1).",
        "01 FILLER.\n05 ITEM PIC X(1).",
        "01 GROUP.\n05 ITEM PIC X(1).\n05 item PIC 9(1).",
        "01 GROUP.\n05 A-B PIC X(1).\n05 a-b PIC X(2).",
    ],
)
def test_missing_group_fields_or_duplicate_names_rejected(tmp_path, source):
    with pytest.raises(ValueError):
        parse_copybook(write_copybook(tmp_path, source))


@pytest.mark.parametrize(
    "value,digits,scale,hex_value",
    [
        ("0", 1, 0, "0c"),
        ("-0.00", 4, 2, "00000d"),
        ("0.00", 4, 2, "00000c"),
        ("9", 1, 0, "9c"),
        ("-9", 1, 0, "9d"),
        ("99.99", 4, 2, "09999c"),
        ("-99.99", 4, 2, "09999d"),
        ("0.01", 4, 2, "00001c"),
        ("1.2300", 4, 2, "00123c"),
        ("1E+2", 3, 0, "100c"),
        ("0.99", 2, 2, "099c"),
        ("9999999999999.99", 15, 2, "999999999999999c"),
        ("-9999999999999.99", 15, 2, "999999999999999d"),
    ],
)
def test_comp3_exact_bytes_and_boundaries(value, digits, scale, hex_value):
    raw = pack_comp3(value, digits, scale)
    assert raw.hex() == hex_value
    result = unpack_comp3(raw, scale, digits=digits)
    assert result == Decimal(value)
    assert result.as_tuple().exponent == -scale
    assert result.is_signed() == Decimal(value).is_signed()


@pytest.mark.parametrize("sign,expected", [(0xC, "12.34"), (0xD, "-12.34"), (0xF, "12.34")])
def test_accepted_comp3_signs(sign, expected):
    assert unpack_comp3(bytes([0x01, 0x23, 0x40 | sign]), 2, digits=4) == Decimal(expected)


@pytest.mark.parametrize("sign", [*range(10), 0xA, 0xB, 0xE])
def test_other_comp3_signs_rejected(sign):
    with pytest.raises(ValueError, match="sign nibble"):
        unpack_comp3(bytes([0x10 | sign]), 0, digits=1)


@pytest.mark.parametrize("nibble", range(0xA, 0x10))
@pytest.mark.parametrize("position", [0, 1, 2])
def test_nondecimal_comp3_digit_nibbles_rejected(nibble, position):
    nibbles = [1, 2, 3, 0xC]
    nibbles[position] = nibble
    raw = bytes([nibbles[0] * 16 + nibbles[1], nibbles[2] * 16 + nibbles[3]])
    with pytest.raises(ValueError, match="digit nibble"):
        unpack_comp3(raw, 0, digits=3)


@pytest.mark.parametrize(
    "raw,digits,scale",
    [
        (b"", None, 0),
        (b"", 1, 0),
        (bytes.fromhex("1c"), 3, 0),
        (bytes.fromhex("001c"), 1, 0),
        (bytes.fromhex("12345c"), 4, 2),
        (bytes.fromhex("001c"), 3, -1),
        (bytes.fromhex("001c"), 3, 4),
        ("001c", 3, 0),
    ],
)
def test_bad_comp3_raw_precision_or_padding_rejected(raw, digits, scale):
    with pytest.raises(ValueError):
        unpack_comp3(raw, scale, digits=digits)


@pytest.mark.parametrize(
    "digits,scale",
    [(0, 0), (-1, 0), (1.5, 0), ("3", 0), (True, 0), (3, -1), (3, 4), (3, 1.5), (3, True), (3, "2")],
)
def test_invalid_precision_arguments_rejected(digits, scale):
    with pytest.raises(ValueError):
        pack_comp3("0", digits, scale)
    with pytest.raises(ValueError):
        unpack_comp3(bytes.fromhex("000c"), scale, digits=digits)


@pytest.mark.parametrize(
    "value",
    [
        "1.234", "-1.234", "0.001", "1E-100000", "100.00", "-100.00",
        "1E+100000", "NaN", "sNaN", "Infinity", "-Infinity", "", "money", None,
    ],
)
def test_pack_rejects_loss_overflow_nonfinite_or_invalid_values(value):
    with pytest.raises(ValueError):
        pack_comp3(value, digits=4, scale=2)


def test_comp3_does_not_depend_on_decimal_context():
    value = Decimal("123456789012345678901234567890.12")
    with localcontext() as context:
        context.prec = 3
        context.traps[Inexact] = True
        raw = pack_comp3(value, digits=32, scale=2)
        assert unpack_comp3(raw, 2, digits=32) == value
        assert pack_comp3("0E+100000", 4, 2) == bytes.fromhex("00000c")
        assert pack_comp3("1.2300", 4, 2) == bytes.fromhex("00123c")
        with pytest.raises(ValueError):
            pack_comp3("1.234", 4, 2)


@pytest.mark.parametrize("row", SAMPLE_ROWS)
def test_all_sample_rows_roundtrip(row):
    fields = parse_copybook("samples/copybooks/ACCOUNT_BALANCE.cbl")
    raw = build_record(row)
    assert len(raw) == 51
    assert parse_record(raw, fields) == row
    assert parse_record(raw, fields, code_page="cp037") == row


@pytest.mark.parametrize("code_page", ["utf-8", "latin1", "cp500", "cp1140", "", None])
def test_unsupported_code_page_rejected(code_page):
    fields = parse_copybook("samples/copybooks/ACCOUNT_BALANCE.cbl")
    with pytest.raises(ValueError, match="code page"):
        parse_record(build_record(SAMPLE_ROWS[0]), fields, code_page=code_page)


@pytest.mark.parametrize("record", [b"", b"\x00" * 50, b"\x00" * 52, "not raw bytes"])
def test_invalid_record_length_or_type_rejected(record):
    fields = parse_copybook("samples/copybooks/ACCOUNT_BALANCE.cbl")
    with pytest.raises(ValueError):
        parse_record(record, fields)


@pytest.mark.parametrize("name", ["branch", "last_txn_date", "sequence_number"])
@pytest.mark.parametrize("bad_byte", [0x40, 0x00, 0x30, 0x60, 0xC1, 0xD1, 0xFA, 0xFF])
def test_corrupt_zoned_bytes_rejected(name, bad_byte):
    fields = parse_copybook("samples/copybooks/ACCOUNT_BALANCE.cbl")
    field = next(field for field in fields if field.name == name)
    raw = bytearray(build_record(SAMPLE_ROWS[0]))
    raw[field.offset] = bad_byte
    with pytest.raises(ValueError, match="zoned"):
        parse_record(raw, fields)


@pytest.mark.parametrize("raw", [bytes.fromhex("11234c"), bytes.fromhex("0a234c"), bytes.fromhex("01234a")])
def test_record_rejects_packed_padding_digit_or_sign_corruption(tmp_path, raw):
    fields = parse_copybook(write_copybook(tmp_path, "01 RECORD.\n05 AMOUNT PIC S9(2)V99 COMP-3."))
    with pytest.raises(ValueError):
        parse_record(raw, fields)


def test_unsigned_comp3_rejects_negative_sign(tmp_path):
    fields = parse_copybook(write_copybook(tmp_path, "01 RECORD.\n05 AMOUNT PIC 9(2) COMP-3."))
    with pytest.raises(ValueError, match="unsigned"):
        parse_record(pack_comp3("-1", 2, 0), fields)
    assert parse_record(bytes.fromhex("001f"), fields) == {"amount": "1"}


@pytest.mark.parametrize(
    "changes",
    [
        {"offset": -1}, {"offset": 1}, {"offset": False}, {"length": 0},
        {"length": 2}, {"length": 1.0}, {"digits": 1}, {"scale": 1},
        {"kind": "unknown"}, {"picture": "X(0)"}, {"picture": None},
        {"name": ""}, {"name": "A"}, {"name": "a-b"}, {"name": "filler"},
    ],
)
def test_malformed_field_layouts_rejected(changes):
    field = replace(FieldLayout("a", "X(1)", "text", 0, 1), **changes)
    with pytest.raises(ValueError):
        record_length([field])
    with pytest.raises(ValueError):
        parse_record(b"\x40", [field])


@pytest.mark.parametrize(
    "fields",
    [
        [],
        [None],
        [FieldLayout("a", "X(1)", "text", 0, 1), FieldLayout("a", "X(1)", "text", 1, 1)],
        [FieldLayout("a", "X(1)", "text", 0, 1), FieldLayout("b", "X(1)", "text", 0, 1)],
        [FieldLayout("a", "X(1)", "text", 0, 1), FieldLayout("b", "X(1)", "text", 2, 1)],
        [FieldLayout("a", "S9(2)V99 COMP-3", "packed_decimal", 0, 2, 4, 2)],
        [FieldLayout("a", "S9(2)V99 COMP-3", "packed_decimal", 0, 3, 5, 2)],
    ],
)
def test_empty_duplicate_overlapping_or_inconsistent_layouts_rejected(fields):
    with pytest.raises(ValueError):
        record_length(fields)
    with pytest.raises(ValueError):
        parse_record(b"", fields)


@pytest.mark.parametrize(
    "name,value",
    [
        ("account_id", "1234567890123"), ("account_id", ""), ("account_id", " " * 12),
        ("account_id", None), ("account_id", "😀"),
        ("status", "AB"), ("status", ""), ("status", " "), ("status", None),
        ("branch", -1), ("branch", 10000), ("branch", 1.5), ("branch", True),
        ("branch", ""), ("branch", " 1"), ("branch", "+1"), ("branch", "１２"),
        ("branch", Decimal("1.5")), ("last_txn_date", 100000000),
        ("sequence_number", 10000000000), ("sequence_number", -1),
        ("current_balance", "1.001"), ("available_limit", "NaN"),
        ("current_balance", "10000000000000.00"),
    ],
)
def test_build_record_rejects_invalid_or_truncated_values(name, value):
    with pytest.raises(ValueError):
        build_record({**SAMPLE_ROWS[0], name: value})


@pytest.mark.parametrize("missing", list(SAMPLE_ROWS[0]))
def test_build_record_requires_all_sample_fields(missing):
    row = dict(SAMPLE_ROWS[0])
    del row[missing]
    with pytest.raises(ValueError, match="Missing sample fields"):
        build_record(row)


@pytest.mark.parametrize("branch,sequence", [(0, 0), (9999, 9999999999), ("0001", "0000000002")])
def test_sample_zoned_boundaries_and_text_padding(branch, sequence):
    row = {
        **SAMPLE_ROWS[0], "account_id": "ABC", "branch": branch, "sequence_number": sequence,
        "current_balance": "0.00", "available_limit": "-9999999999999.99",
    }
    raw = build_record(row)
    assert len(raw) == 51
    assert raw[:12] == "ABC         ".encode("cp037")
    fields = parse_copybook("samples/copybooks/ACCOUNT_BALANCE.cbl")
    assert parse_record(raw, fields) == {**row, "branch": int(branch), "sequence_number": int(sequence)}
