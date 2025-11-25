"""Phase 1 analysis: sampling files, building FileBlock signatures."""

from .block_planner import BlockPlanner, PlannedBlock
from .engine import AnalysisEngine
from .line_counter import LineCounter

__all__ = ["AnalysisEngine", "BlockPlanner", "PlannedBlock", "LineCounter"]
