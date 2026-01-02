from typing import List, Union, Mapping, Any

import pandas
from pandas._version import get_versions

__all__ = ["read_excel"]

_major, _minor, _patch = get_versions()["version"].split(".")


class PandasVersion:
    major = int(_major)
    minor = int(_minor)
    patch = int(_patch)


read_excel = pandas.read_excel

if PandasVersion.major == 1 and PandasVersion.minor >= 3:

    from pandas._typing import (
        Scalar,
        DtypeArg,
        StorageOptions,
    )
    from pandas.io.excel import ExcelFile
    from pandas.io.excel._base import _read_excel_doc
    from pandas.io.excel._openpyxl import OpenpyxlReader
    from pandas.util._decorators import (
        Appender,
        deprecate_nonkeyword_arguments,
    )

    class OpenpyxlReaderExt(OpenpyxlReader):
        def get_sheet_data(
                self, sheet, convert_float: bool, file_rows_needed: int = None
        ) -> List[List[Scalar]]:
            if self.book.read_only:
                sheet.reset_dimensions()

            data: List[List[Scalar]] = []
            last_row_with_data = -1
            for row_number, row in enumerate(sheet.rows):
                converted_row = [
                    self._convert_cell(cell, convert_float) for cell in row
                ]
                # while converted_row and converted_row[-1] == "":
                #     # trim trailing empty elements
                #     converted_row.pop()
                if converted_row:
                    last_row_with_data = row_number
                data.append(converted_row)
                if file_rows_needed is not None and len(data) >= file_rows_needed:
                    break

            # Trim trailing empty rows
            data = data[: last_row_with_data + 1]

            if len(data) > 0:
                # extend rows to max width
                max_width = max(len(data_row) for data_row in data)
                if min(len(data_row) for data_row in data) < max_width:
                    empty_cell: List[Scalar] = [""]
                    data = [
                        data_row + (max_width - len(data_row)) * empty_cell
                        for data_row in data
                    ]

            return data

    class ExcelFileExt(ExcelFile):
        from pandas.io.excel._odfreader import ODFReader
        from pandas.io.excel._pyxlsb import PyxlsbReader
        from pandas.io.excel._xlrd import XlrdReader

        _engines: Mapping[str, Any] = {
            "xlrd": XlrdReader,
            "openpyxl": OpenpyxlReaderExt,
            "odf": ODFReader,
            "pyxlsb": PyxlsbReader,
        }

    @deprecate_nonkeyword_arguments(allowed_args=["io", "sheet_name"], version="2.0")
    @Appender(_read_excel_doc)
    def _read_excel(
        io,
        sheet_name=0,
        header=0,
        names=None,
        index_col=None,
        usecols=None,
        squeeze=False,
        dtype: Union[DtypeArg, None] = None,
        engine=None,
        converters=None,
        true_values=None,
        false_values=None,
        skiprows=None,
        nrows=None,
        na_values=None,
        keep_default_na=True,
        na_filter=True,
        verbose=False,
        parse_dates=False,
        date_parser=None,
        thousands=None,
        comment=None,
        skipfooter=0,
        convert_float=None,
        mangle_dupe_cols=True,
        storage_options: StorageOptions = None,
    ):
        should_close = False
        if not isinstance(io, (ExcelFile, ExcelFileExt)):
            should_close = True
            io = ExcelFileExt(io, storage_options=storage_options, engine=engine)
        elif engine and engine != io.engine:
            raise ValueError(
                "Engine should not be specified when passing "
                "an ExcelFile - ExcelFile already has the engine set"
            )

        try:
            data = io.parse(
                sheet_name=sheet_name,
                header=header,
                names=names,
                index_col=index_col,
                usecols=usecols,
                squeeze=squeeze,
                dtype=dtype,
                converters=converters,
                true_values=true_values,
                false_values=false_values,
                skiprows=skiprows,
                nrows=nrows,
                na_values=na_values,
                keep_default_na=keep_default_na,
                na_filter=na_filter,
                verbose=verbose,
                parse_dates=parse_dates,
                date_parser=date_parser,
                thousands=thousands,
                comment=comment,
                skipfooter=skipfooter,
                convert_float=convert_float,
                mangle_dupe_cols=mangle_dupe_cols,
            )
        finally:
            # make sure to close opened file handles
            if should_close:
                io.close()
        return data

    read_excel = _read_excel
