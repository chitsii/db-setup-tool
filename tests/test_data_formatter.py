from textwrap import dedent

from data_formatter import CSVFormatter


def test_csv_format():
    # 準備
    s = dedent(
        """\
    'name','age','gender'
    Alice,30,female
    Bob,40,male
    ,0,unknown
    ,,
    """
    )
    fmt = CSVFormatter()
    fmt.has_header = True

    # 実行
    res = fmt.parse(s)

    # 確認
    assert len(res) == 3, "行数が違います"
    assert len(res[0].keys()) == 3, "カラム数が違います"
    assert res == [
        {"name": "Alice", "age": "30", "gender": "female"},
        {"name": "Bob", "age": "40", "gender": "male"},
        {"name": "", "age": "0", "gender": "unknown"},
    ]

def test_csv_format_セル内改行あり():
    # 準備
    s = dedent(
        """\
    'name','age','gender'
    Alice,30,female
    Bob,40,male
    ,0,'unk
    nown'
    ,,
    """
    )
    fmt = CSVFormatter()
    fmt.has_header = True

    # 実行
    res = fmt.parse(s)

    # 確認
    assert len(res) == 3, "行数が違います"
    assert len(res[0].keys()) == 3, "カラム数が違います"
    assert res == [
        {"name": "Alice", "age": "30", "gender": "female"},
        {"name": "Bob", "age": "40", "gender": "male"},
        {"name": "", "age": "0", "gender": "unk\nnown"},
    ]

def test_csv_format_ヘッダなし():
    # 準備
    s = dedent(
        """\
    Alice,30,female
    Bob,40,male
    ,0,unknown
    ,,
    """
    )
    fmt = CSVFormatter()
    fmt.has_header = False

    # 実行
    res = fmt.parse(s)

    # 確認
    assert len(res) == 3, "行数が違います"
    assert len(res[0].keys()) == 3
    assert res == [
        {"field_1": "Alice", "field_2": "30", "field_3": "female"},
        {"field_1": "Bob", "field_2": "40", "field_3": "male"},
        {"field_1": "", "field_2": "0", "field_3": "unknown"},
    ]
