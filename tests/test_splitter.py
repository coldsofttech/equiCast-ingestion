import json
import os

import pytest

from equicast_ingestion import Splitter


@pytest.mark.noca
def test_invalid_mode(tmp_path):
    file_path = tmp_path / "input.json"
    file_path.write_text(json.dumps(["AAPL", "MSFT"]))

    splitter = Splitter(mode="crypto", filepath=str(file_path), pref_chunk_size=2)
    with pytest.raises(ValueError, match="mode must be 'stock' or 'fx'"):
        splitter.split()


@pytest.mark.noca
def test_fx_not_implemented(tmp_path):
    file_path = tmp_path / "input.json"
    file_path.write_text(json.dumps(["EURUSD", "USDJPY"]))

    splitter = Splitter(mode="fx", filepath=str(file_path), pref_chunk_size=2)
    with pytest.raises(NotImplementedError):
        splitter.split()


@pytest.mark.noca
def test_stock_split_creates_chunks(tmp_path):
    input_data = ["AAPL", "MSFT", "AAPL", "GOOG", "TSLA"]
    file_path = tmp_path / "input.json"
    file_path.write_text(json.dumps(input_data))

    splitter = Splitter(mode="stock", filepath=str(file_path), pref_chunk_size=2)
    splitter.output_dir = str(tmp_path / "stock_chunks")
    os.makedirs(splitter.output_dir, exist_ok=True)
    splitter.split()

    chunk_files = sorted(tmp_path.joinpath("stock_chunks").glob("chunk_*.json"))
    assert len(chunk_files) == 2

    first_chunk = json.loads(chunk_files[0].read_text())
    second_chunk = json.loads(chunk_files[1].read_text())
    assert first_chunk == ["AAPL", "MSFT"]
    assert second_chunk == ["GOOG", "TSLA"]

    stock_chunks = json.loads(open("stock_chunks.json").read())
    assert stock_chunks == [1, 2]
    os.remove("stock_chunks.json")


@pytest.mark.noca
def test_stock_split_respects_max_chunks(tmp_path, capsys):
    input_data = [f"TICK{i}" for i in range(600)]
    file_path = tmp_path / "input.json"
    file_path.write_text(json.dumps(input_data))

    splitter = Splitter(mode="stock", filepath=str(file_path), pref_chunk_size=1)
    splitter.output_dir = str(tmp_path / "stock_chunks")
    os.makedirs(splitter.output_dir, exist_ok=True)
    splitter.max_chunks = 10
    splitter.split()

    chunk_files = sorted(tmp_path.joinpath("stock_chunks").glob("chunk_*.json"))
    assert len(chunk_files) <= 10

    captured = capsys.readouterr()
    assert "⚠️ Too many chunks" in captured.out
    assert "Increasing chunk size" in captured.out
