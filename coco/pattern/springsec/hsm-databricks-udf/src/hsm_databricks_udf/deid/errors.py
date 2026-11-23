"""
De-identification errors, as com.hsm.client.deid.DeidException and
DeidValidationException (java/docs/DEIDENTIFICATION.md principle 5).

Messages name the field and row number, never the value: they end up in logs.
"""

from __future__ import annotations


class DeidError(RuntimeError):
    """A value or key can't be processed. ``reason`` must never contain the value."""

    def __init__(self, reason: str, field: str | None = None, row: int = -1, page_index: int = -1):
        self.reason = reason
        self.field = field
        self.row = row
        self.page_index = page_index
        super().__init__(self._message())

    @classmethod
    def bad_value(cls, reason: str) -> "DeidError":
        return cls(reason)

    def at_page_index(self, index: int) -> "DeidError":
        """The same failure, at position ``index`` of the page being processed."""
        e = DeidError(self.reason, self.field, self.row, index)
        e.__cause__ = self.__cause__
        return e

    def at(self, field: str, row: int) -> "DeidError":
        """The same failure, located at a field and a 1-based row number (row <= 0: no row)."""
        e = DeidError(self.reason, field, row, self.page_index)
        e.__cause__ = self.__cause__
        return e

    def _message(self) -> str:
        if self.field is None:
            return self.reason
        return "field '" + self.field + "'" + (", row " + str(self.row) if self.row > 0 else "") + ": " + self.reason


class DeidValidationError(ValueError):
    """A profile can't run. ``problems`` lists every problem found, never a value."""

    def __init__(self, problems: list[str]):
        self.problems = list(problems)
        super().__init__(str(len(self.problems)) + " profile problem(s): " + "; ".join(self.problems))
