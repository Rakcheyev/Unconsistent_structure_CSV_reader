from __future__ import annotations

from core.analysis.block_planner import BlockPlanner
from core.analysis.line_counter import LineCounter


def test_line_counter_counts_lines(tmp_path):
    payload = "alpha\nbravo\ncharlie"
    path = tmp_path / "sample.csv"
    path.write_text(payload, encoding="utf-8")
    counter = LineCounter()
    assert counter.count(path) == 3


def test_block_planner_limits_buffer(tmp_path):
    path = tmp_path / "large_lines.csv"
    line = "x" * 256 + "\n"
    path.write_text(line * 50, encoding="utf-8")
    planner = BlockPlanner(block_size=10, min_gap_lines=5, buffer_limit_bytes=512)
    plan = planner.plan(total_lines=50)
    buffers = list(planner.iter_block_buffers(path, plan, encoding="utf-8", errors="strict"))
    assert buffers
    for planned, lines in buffers:
        total_bytes = sum(len(l.encode("utf-8")) for l in lines)
        assert total_bytes <= 512
        assert planned.end_line - planned.start_line + 1 <= 10
