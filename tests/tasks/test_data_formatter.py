import pytest

from textwrap import dedent
from io import BytesIO

from tasks.data_formatter import CSVFormatter

@pytest.mark.unit
@pytest.mark.normal
def test_csv_format():
    # 準備
    s = dedent(
        """\
    "name","age","gender"
    "Alice",30,"female"
    "Bob",40,"male"
    "",0,"unknown"
    ,,
    """
    )

    # 実行
    fmt = CSVFormatter(
        encoding="utf-8",
        has_header=True,
        column_names=None
    )
    res = fmt.parse(
        bytes_input=BytesIO(s.encode("utf-8"))
    )

    # 確認
    assert len(res) == 3, "行数が違います"
    assert len(res[0].keys()) == 3, "カラム数が違います"
    assert res == [
        {"name": "Alice", "age": "30", "gender": "female"},
        {"name": "Bob", "age": "40", "gender": "male"},
        {"name": "", "age": "0", "gender": "unknown"},
    ]

@pytest.mark.unit
@pytest.mark.normal
def test_csv_format_セル内改行あり():
    # 準備
    s = dedent(
        """\
    "name","age","gender"
    "Alice",30,"female"
    "Bob",40,"male"
    "",0,"unk
    nown"
    """
    )

    # 実行
    fmt = CSVFormatter(
        encoding="utf-8",
        has_header=True,
        column_names=None
    )
    res = fmt.parse(
        bytes_input=BytesIO(s.encode("utf-8")),
    )

    # 確認
    assert len(res) == 3, "行数が違います"
    assert len(res[0].keys()) == 3, "カラム数が違います"
    assert res == [
        {"name": "Alice", "age": "30", "gender": "female"},
        {"name": "Bob", "age": "40", "gender": "male"},
        {"name": "", "age": "0", "gender": "unk\nnown"},
    ]

@pytest.mark.unit
@pytest.mark.normal
def test_csv_format_ヘッダなし():
    # 準備
    s = dedent(
        """\
    "Alice",30,"female"
    "Bob",40,"male"
    "",0,"unknown"
    """
    )

    # 実行
    fmt = CSVFormatter(
        encoding="utf-8",
        has_header=False,
        column_names=["name", "age", "gender"]
    )
    res = fmt.parse(
        bytes_input=BytesIO(s.encode("utf-8")),
    )

    # 確認
    assert len(res) == 3, "行数が違います"
    assert len(res[0].keys()) == 3
    assert res == [
        {"name": "Alice", "age": "30", "gender": "female"},
        {"name": "Bob", "age": "40", "gender": "male"},
        {"name": "", "age": "0", "gender": "unknown"},
    ]
