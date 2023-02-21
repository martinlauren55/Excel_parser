from typing import Optional

import pandas as pd

from datamodels.fact_forecast_lib import Company, FactForecast, FactForecastBase


class FactForecastParser:
    def __init__(self, input_data: str, output_data: str, set_data: Optional[list] = None):
        """

        :param input_data: - Path to file Excel
        :param output_data: Path to file Database
        :param set_data: By default, the column names (table name in the database) and the column number for data1 and
                        data2 are defined. When scaling the parser file, you need to override the default value by
                        replacing or adding new columns.
        """
        self.input_data = input_data
        self.output_data = output_data
        self._set_data = set_data
        if not self._set_data:
            self._set_data = [{"table_name": "fact_Qliq", "col_num_data1": 3, "col_num_data2": 4},
                              {"table_name": "fact_Qoil", "col_num_data1": 5, "col_num_data2": 6},
                              {"table_name": "forecast_Qliq", "col_num_data1": 7, "col_num_data2": 8},
                              {"table_name": "forecast_Qoil", "col_num_data1": 9, "col_num_data2": 10}]

    def _parse_company(self):
        self._company = Company(input_data=self.input_data, output_data=self.output_data)
        self._company.parse()

    def parse(self):
        """Run parse file"""
        self._parse_company()
        self._parse_fact_forcast()

    def _parse_fact_forcast(self):
        if self._set_data:
            for item in self._set_data:
                FactForecast(input_data=self.input_data,
                             output_data=self.output_data,
                             col_num_data1=item['col_num_data1'],
                             col_num_data2=item['col_num_data2'],
                             table_name=item['table_name']).parse()

    def get_total(self):
        """Returns the resulting total for all tables to the console"""
        if self._set_data:
            for item in self._set_data:
                df = FactForecastBase(input_data=self.input_data,
                                     output_data=self.output_data).get_all_data_table(item['table_name'])
                df["date"] = pd.to_datetime(df["date"])
                df["date"] = df["date"].dt.date
                df["data1"] = pd.to_numeric(df["data1"], downcast='float')
                df["data2"] = pd.to_numeric(df["data2"], downcast='float')
                df["total"] = df[["data1", "data2"]].sum(axis=1)
                print(f"\nTotal for \"{item['table_name']}\"")
                for i in range(len(df)):
                    print(df.iloc[i]['date'], df.iloc[i]['total'])