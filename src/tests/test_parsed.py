import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from src.scripts.parsed import ParsedDf, get_my_env_var, clickhouse_client, unified_list_line_name, \
    unified_list_line_name_skip, MissingEnvironmentVariable


@pytest.fixture
def sample_df():
    data = {
        'consignment': ['CSYTVS134948', 'МАГВОС24290002'],
        'goods_name': ['АВТОЗАПЧАСТИ (ТОРМОЗНЫЕ КОЛОДКИ)', 'КОНТЕЙНЕРЫ УНИВЕРСАЛЬНЫЕ НИЕ СОБСТВЕННЫЕ'],
        'line': ['ARKAS', 'ARKAS'],
        'direction': ['export', 'cabotage'],
        'tracking_seaport': [None, None]
    }
    return pd.DataFrame(data)


@pytest.fixture
def parsed_df(sample_df):
    return ParsedDf(sample_df)


# ---- TESTS ----
# Тесты для get_my_env_var
@pytest.mark.parametrize("env_var, value", [
    ("HOST", "localhost"),
    ("DATABASE", "test_db")
])
def test_get_my_env_var(env_var, value):
    with patch.dict(os.environ, {env_var: value}):
        assert get_my_env_var(env_var) == value


def test_get_my_env_var_missing():
    with pytest.raises(MissingEnvironmentVariable):
        get_my_env_var("NON_EXISTENT_VAR")


# Тесты для clickhouse_client
@patch("src.scripts.parsed.get_client")
def test_clickhouse_client(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    client = clickhouse_client()
    assert client is not None
    mock_get_client.assert_called_once()


@patch("src.scripts.parsed.get_client", side_effect=Exception("Connection failed"))
def test_clickhouse_client_failure(mock_get_client):
    with pytest.raises(SystemExit):
        clickhouse_client()


# Тесты для unified_list_line_name
@patch("src.scripts.parsed.clickhouse_client")
def test_unified_list_line_name(mock_clickhouse_client):
    mock_clickhouse_client().query.return_value.result_rows = [("SAFETRANS", "LINE_A"), ("ARKAS", "LINE_B")]
    expected = {"LINE_A": ["SAFETRANS"], "LINE_B": ["ARKAS"]}
    assert unified_list_line_name() == expected


@patch("src.scripts.parsed.clickhouse_client")
def test_unified_list_line_name_skip(mock_clickhouse_client):
    mock_clickhouse_client().query.return_value.result_rows = [("MSC", "MSC"), ("ARKAS", "ARKAS")]
    expected = {"ARKAS": ["ПОРОЖ", "ПРОЖ"], "MSC": ["ПОРОЖ", "ПРОЖ"]}
    result = unified_list_line_name_skip()
    assert result == expected


@pytest.mark.parametrize('row,answer', [
    ({"line": "ARKAS"}, False),
    ({"line": "ARKA"}, True)
])
def test_check_lines(row: dict, answer: str, parsed_df: ParsedDf, monkeypatch):
    monkeypatch.setattr("src.scripts.parsed.unified_list_line_name_skip", lambda: {"ARKAS": ["ARKAS"]})
    assert parsed_df.check_lines(row) == answer


@pytest.mark.parametrize("direction,answer", [
    ("import", "import"),
    ("export", "export"),
    ("cabotage", "cabotage")])
def test_get_direction(direction: str, answer: str, parsed_df):
    assert parsed_df.get_direction(direction) == answer


@pytest.mark.parametrize("consignment,answer", [
    ("VX61CT24000372,VX61CT24000372", "VX61CT24000372"),
    ("VX61CT24000372", "VX61CT24000372")
])
def test_get_number_consignment(consignment: str, answer: str, parsed_df: ParsedDf):
    assert parsed_df.get_number_consignment(consignment) == answer


def mock_get_line_unified(item, line_name: str):
    return line_name


# @pytest.mark.parametrize("row,consignment,answer", [
#     ({"line": "ARKAS", "consignment": "ARKAS123456", "direction": "export"},
#      "ARKAS123456", {"line": "ARKAS", "consignment": "ARKAS123456", "direction": "export"}),
#     ({"line": "ARKAS", "consignment": "ARKAS123456", "direction": "import"},
#      "ARKAS123456", {"line": "ARKAS", "consignment": "ARKAS123456", "direction": "import"})
# ])
# def test_body(row: str, consignment: str, answer: dict, parsed_df: ParsedDf, monkeypatch):
#     monkeypatch.setattr('src.scripts.parsed.get_line_unified', mock_get_line_unified)
#     monkeypatch.setattr('src.scripts.parsed.unified_list_line_name', lambda: {"ARKAS": ["ARKAS"]})
#     result = parsed_df.body(row, consignment)
#     print(result)
#     assert result == answer


@pytest.mark.parametrize("row,consignment,answer", [
    ({"line": "ARKAS", "consignment": "ARKAS123456", "direction": "import"}, "consignment", {"PORT"}),
])
@patch('requests.post')
def test_get_port_with_recursion(mock_post, row: dict, consignment: str, answer, parsed_df):
    mock_response = MagicMock()
    mock_response.json.return_value = answer
    mock_post.return_value = mock_response
    with patch.object(ParsedDf, "body", return_value=row):
        port = parsed_df.get_port_with_recursion(1, row, consignment)
    assert port == answer
    mock_post.assert_called()


def test_add_new_columns(parsed_df):
    parsed_df.add_new_columns()
    assert "is_auto_tracking" in parsed_df.df
