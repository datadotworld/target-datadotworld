from csv import DictWriter
from json import dump


class StreamDictWriter:
    """
    A writer that streams dict rows into a data.world file
    """
    def __init__(self, ownerid, datasetid, filename, writer, f):
        self._ownerid = ownerid
        self._datasetid = datasetid
        self._filename = filename
        self._writer = writer
        self._f = f
        self._rows_written = 0

    def do_write_row(self, writer, row_dict):
        raise NotImplementedError()

    def write_row(self, row_dict):
        self.do_write_row(self._writer, row_dict)
        self._rows_written += 1

    def rows_written(self):
        return self._rows_written

    def ownerid(self):
        return self._ownerid

    def datasetid(self):
        return self._datasetid

    def filename(self):
        return self._filename

    def close(self):
        self._f.close()


class CsvStreamDictWriter(StreamDictWriter):
    """
    A writer that streams dict rows as a CSV into a data.world file
    """
    def __init__(self, ownerid, datasetid, filename, f, fieldnames):
        writer = DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        super(CsvStreamDictWriter, self).__init__(
            ownerid, datasetid, filename, writer, f)

    def do_write_row(self, writer, row_dict):
        writer.writerow(row_dict)


class JsonlStreamDictWriter(StreamDictWriter):
    """
    A writer that streams dict rows as a Json Lines file into a data.world file
    """
    def __init__(self, ownerid, datasetid, filename, f):
        super(JsonlStreamDictWriter, self).__init__(
            ownerid, datasetid, filename, f, f)

    def do_write_row(self, writer, row_dict):
        dump(row_dict, writer)
        writer.write('\n')
