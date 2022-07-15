"""
Template Component main class.

"""
import csv
import json
import logging
from pathlib import Path
from typing import List
from tqdm import tqdm

from csv2json.hone_csv2json import Csv2JsonConverter
from keboola.component.base import ComponentBase
from keboola.component.dao import TableDefinition, FileDefinition
from keboola.component.exceptions import UserException

# configuration variables
KEY_FORMAT = 'format'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters
        # Access parameters in data/config.json
        if params.get(KEY_FORMAT):
            logging.info(f"Format setting is: {params.get(KEY_FORMAT)}")

        input_tables = self.get_input_tables_definitions()

        for table in input_tables:
            _format = self.configuration.parameters[KEY_FORMAT]
            logging.info(f"Processing table {table.name} using format: {_format}")
            if _format == 'pricehistory':
                self._generate_price_history(table)
            elif _format == 'metadata':
                self._generate_metadata(table)
            else:
                raise UserException(f"Wrong parameter in data/config.json {_format}. "
                                    "Viable parameters are: pricehistory/metadata")

        logging.info("Parsing finished successfully!")

    def _validate_expected_columns(self, table_type, table: TableDefinition, expected_columns: List[str]):
        errors = []
        # validate
        for c in expected_columns:
            if c not in table.columns:
                errors.append(c)

        if errors:
            error = f'Some required columns are missing for format {table_type}. ' \
                    f'Missing columns: [{"; ".join(errors)}] '
            raise UserException(error)

    def _write_json_content_to_file(self, file: FileDefinition, content: dict):
        Path(file.full_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file.full_path, 'w+', encoding='utf-8') as outp:
            json.dump(content, outp)

    def _generate_price_history(self, table: TableDefinition):
        expected_columns = ['shop_id', 'slug', 'json']
        # validate
        self._validate_expected_columns('pricehistory', table, expected_columns)

        with open(table.full_path, 'r', encoding='utf-8') as inp:
            reader = csv.DictReader(inp)
            for row in tqdm(reader):
                out_file = self.create_out_file_definition(f'{row["shop_id"]}/{row["slug"]}/price-history.json')
                content = json.loads(row['json'])
                self._write_json_content_to_file(out_file, content)

    def _generate_metadata_content(self, columns, row: List[str]):
        converter = Csv2JsonConverter(headers=columns, delimiter='__')
        return converter.convert_row(row, [], '__', infer_undefined=True)

    def _generate_metadata(self, table: TableDefinition):
        expected_columns = ['slug', 'shop_id']
        # validate
        self._validate_expected_columns('metadata', table, expected_columns)

        with open(table.full_path, 'r') as inp:
            reader = csv.DictReader(inp)
            for row in reader:
                out_file = self.create_out_file_definition(f'{row["shop_id"]}/{row["slug"]}/meta.json')
                # remove columns:
                for c in expected_columns:
                    row.pop(c, None)

                content = self._generate_metadata_content(list(row.keys()), list(row.values()))
                self._write_json_content_to_file(out_file, content[0])


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
