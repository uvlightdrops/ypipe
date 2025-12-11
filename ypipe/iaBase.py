
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

from ypipe.dialogs import QuitScreen


class iaBase:
    def set_col_attrs(self, dt, col_widths=None):
        logger.debug("Setting column attributes with col_widths: %s", col_widths)
        default_width = col_widths.pop('default', 10)
        for col_name, width in col_widths.items():
            value = width if col_name in self.columns else default_width
            logger.debug("Setting width of column %s to %s", col_name, value)
            if col_name in dt.columns:
                dt.columns[col_name].width = value

    def truncate_cell(self, cell, col, width):
        if isinstance(cell, float) and cell.is_integer():
            cell = str(int(cell))
        else:
            cell = str(cell)
        if isinstance(cell, str) and len(cell) > width+2:
            return cell[:width-2] + '..'
        return cell

    def truncate_cell_all(self, row):
        values = []
        for col_name, width in self.col_widths.items():
            clean = self.truncate_cell(row[col_name], col_name, width)
            row[col_name] = clean
            if col_name in self.columns:
                values.append(clean)
        return row, values

    def truncate_all(self, row):
        values = []
        for col_name in self.columns:
            width = self.col_widths.get(col_name, 10)
            clean = self.truncate_cell(row[col_name], col_name, width)
            row[col_name] = clean
            values.append(clean)
        return row, values

    def update_current_line(self):
        row = self.df.iloc[self.main_table.cursor_row]
        logger.debug("Hightlighting row %s", self.main_table.cursor_row)
        row, values = self.truncate_all(row)
        self.current_line_dt.clear()
        self.current_line_dt.add_row(*values)
        self.current_line_dt.refresh()
