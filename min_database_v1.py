from dataclasses import dataclass
from typing import Any, List


@dataclass
class Cell:
    type: Any
    size: Any
    val: Any


@dataclass
class Row:
    cells: List[Cell]


@dataclass
class RowSet:
    rows: List[Row]


class CSVDataBase:
    def __init__(self):
        ...

    def read_table_from_csv(self, filepath) -> RowSet:
        """
        read a csv file and return a table
        """

        def csv_to_row_set(filepath):
            ...

        return csv_to_row_set(filepath)


@dataclass
class LimitOperator:
    row_set: RowSet
    limit_size: int

    def __call__(self) -> RowSet:
        return RowSet(self.row_set.rows[:self.limit_size])

class RowComputeOperator:
    def __init__(self, compute_cell_val):
        self.compute_cell_val = compute_cell_val
    def process(self, row):
        row.cells.append(self.compute_cell_val(row))
        return row
@dataclass
class SelectOperator:
    indexes: Any
    
    def process(self, row):
        return Row(cells=[row.cells[i] for i in self.indexes])

def projectionOperator(row_set, projection_operators):
    new_rows = []
    for row in row_set.rows:
        for op in projection_operators:
            row = op.process(row)
        new_rows.append(row)
    return RowSet(new_rows)

def FilterOperator(row_set, filter_col_index):
    new_rows = []
    for row in row_set.rows:
        if row.cells[filter_col_index].val:
            new_rows.append(row)
    return RowSet(new_rows)


def SortOperator(row_set, sort_index, order_by="ASC"):
    new_row_set = []
    index_map = {}
    cells = []
    for i, row in enumerate(row_set):
        index_map[row[sort_index]] = i
        cells.append(row.cells[sort_index])
    cells.sort(reverse=(order_by=="DESC"))
    for cell in cells:
        new_row_set.append(row_set[index_map[cell]])
    return new_row_set
