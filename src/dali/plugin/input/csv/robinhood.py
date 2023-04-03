# Copyright 2022 macanudo527
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The Consolidated CSV can be found under the "Tax center" tab
# Transfer CSV format: 1009-B, ACCOUNT NUMBER, TAX YEAR, DATE ACQUIRED, SALE DATE, DESCRIPTION, SHARES, COST BASIS, SALES PRICE, TERM, ORDINARY, FED TAX WITHHELD, WASH AMT DISALLOWED, ACCRDMKTDISCOUNT, FORM8949CODE, GROSSPROCEEDSINDICATOR, LOSSNOTALLOWED, NON COVERED, BASIS NOT SHOWN, FORM 1099 NOT REC, COLLECTIBLE, QOF, PROFIT, UNRELPROFITPREV, UNRELPROFIT, AGGPROFIT, STATECODE, FATCA, STATEIDNUM, STATETAXWHELD, PAYER FED ID, PAYER NAME1, PAYER NAME2, PAYER ADDR1, PAYER ADDR2, PAYER ADDR3, PAYER CITY, PAYER STATE, PAYER ZIP, PAYER PHONE

import logging
from csv import reader
from typing import List, Optional

from rp2.abstract_country import AbstractCountry
from rp2.logger import create_logger

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction


class InputPlugin(AbstractInputPlugin):

    __ROBINHOOD: str = "Robinhood"
    __ROBINHOOD_PLUGIN: str = "Robinhood_CSV"

    __1099_B_INDEX: int = 0
    __ACCOUNT_NUMBER_INDEX: int = 1
    __TAX_YEAR_INDEX: int = 2
    __DATE_ACQUIRED_INDEX: int = 3
    __SALE_DATE_INDEX: int = 4
    __DESCRIPTION_INDEX: int = 5
    __SHARES_INDEX: int = 6
    __COST_BASIS_INDEX: int = 7
    __SALES_PRICE_INDEX: int = 8
    __TERM_INDEX: int = 9
    __ORDINARY_INDEX: int = 10
    __FED_TAX_WITHHELD_INDEX: int = 11
    __WASH_AMT_DISALLOWED_INDEX: int = 12 
    __ACCRDMKTDISCOUNT_INDEX: int = 13
    __FORM8949CODE_INDEX: int = 14
    __GROSSPROCEEDSINDICATOR_INDEX: int = 15
    __LOSSNOTALLOWED_INDEX: int = 16
    __NON_COVERED_INDEX: int = 17
    __BASIS_NOT_SHOWN_INDEX: int = 18
    __FORM_1099_NOT_REC_INDEX: int = 19
    __COLLECTIBLE_INDEX: int = 20
    __QOF_INDEX: int = 21
    __PROFIT_INDEX: int = 22
    __UNRELPROFITPREV_INDEX: int = 23
    __UNRELPROFIT_INDEX: int = 24
    __AGGPROFIT_INDEX: int = 25
    __STATECODE_INDEX: int = 26
    __FATCA_INDEX: int = 27
    __STATEIDNUM_INDEX: int = 28
    __STATETAXWHELD_INDEX: int = 29 
    __PAYER_FED_ID_INDEX: int = 30
    __PAYER_NAME1_INDEX: int = 31
    __PAYER_NAME2_INDEX: int = 32
    __PAYER_ADDR1_INDEX: int = 33
    __PAYER_ADDR2_INDEX: int = 34
    __PAYER_ADDR3_INDEX: int = 35
    __PAYER_CITY_INDEX: int = 36
    __PAYER_STATE_INDEX: int = 37
    __PAYER_ZIP_INDEX: int = 38
    __PAYER_PHONE_INDEX: int = 39

    #__TIMESTAMP_INDEX: int = 0
    #__RECEIVED_AMOUNT: int = 1
    #__TRANSACTION_TYPE: int = 1
    #__ASSET_RECEIVED: int = 2
    #__AMOUNT_TRANSFERED: int = 2
    #__SENT_AMOUNT: int = 3
    #__ASSET_TRANSFERED: int = 3
    #__ASSET_SENT: int = 4
    #__CHAIN_USED: int = 4
    #__FEE_AMOUNT: int = 5
    #__TXN_ID: int = 5
    #__FEE_ASSET: int = 6

    __DELIMITER: str = ","

    # Keywords
    __DEPOSIT: str = "DEPOSIT"
    __WITHDRAWAL: str = "WITHDRAWAL"

    def __init__(
        self,
        account_holder: str,
        consolidated_csv_file: Optional[str] = None,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__consolidated_csv_file: Optional[str] = consolidated_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__ROBINHOOD_PLUGIN}/{self.account_holder}")

    def load(self, country: AbstractCountry) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        if self.__consolidated_csv_file:
            result += self.parse_consolidated_file(self.__consolidated_csv_file)

        return result

    def parse_consolidated_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                in_crypto_fee: str = "0"
                out_crypto_fee: str = "0"

                if line[self.__DATE_ACQUIRED_INDEX] == None:
                    if  and line[self.__COST_BASIS_INDEX] == 0:
                        # This is a deposit?
                        result.append(
                            IntraTransaction(
                                plugin=self.__ROBINHOOD_PLUGIN,
                                unique_id=line[self.__TXN_ID],
                                raw_data=raw_data,
                                timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                                asset=asset,
                                from_exchange=Keyword.UNKNOWN.value,
                                from_holder=Keyword.UNKNOWN.value,
                                to_exchange=self.__ROBINHOOD,
                                to_holder=self.account_holder,
                                spot_price=Keyword.UNKNOWN.value,
                                crypto_sent=Keyword.UNKNOWN.value,
                                crypto_received=str(line[self.__AMOUNT_TRANSFERED]),
                            )
                        )
                    else:
                        # This is a withdrawal?
                        result.append(
                            IntraTransaction(
                                plugin=self.__ROBINHOOD_PLUGIN,
                                unique_id=line[self.__TXN_ID],
                                raw_data=raw_data,
                                timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                                asset=asset,
                                from_exchange=self.__ROBINHOOD,
                                from_holder=self.account_holder,
                                to_exchange=Keyword.UNKNOWN.value,
                                to_holder=Keyword.UNKNOWN.value,
                                spot_price=Keyword.UNKNOWN.value,
                                crypto_sent=str(line[self.__AMOUNT_TRANSFERED]),
                                crypto_received=Keyword.UNKNOWN.value,
                            )
                        )
                else:
                    result.append(
                        InTransaction(
                            plugin=self.__ROBINHOOD_PLUGIN,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{line[self.__DATE_ACQUIRED_INDEX]} -00:00",
                            asset=line[self.__DESCRIPTION_INDEX],
                            exchange=self.__ROBINHOOD,
                            holder=self.account_holder,
                            transaction_type=Keyword.BUY.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_in=line[self.__SHARES_INDEX],
                            crypto_fee=in_crypto_fee,
                            notes=None,
                        )
                    )

                    result.append(
                        OutTransaction(
                            plugin=self.__ROBINHOOD_PLUGIN,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{line[self.__SALE_DATE_INDEX]} -00:00",
                            asset=line[self.__DESCRIPTION_INDEX],
                            exchange=self.__ROBINHOOD,
                            holder=self.account_holder,
                            transaction_type=Keyword.SELL.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_out_no_fee=line[self.__SHARES_INDEX],
                            crypto_out_with_fee=line[self.__SHARES_INDEX],
                            crypto_fee=out_crypto_fee,
                            notes=None,
                        )
                    )

            return result

    def parse_transfers_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:

                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                asset: str = (
                    line[self.__ASSET_TRANSFERED][: -len(line[self.__CHAIN_USED])]
                    if (line[self.__ASSET_TRANSFERED].endswith(line[self.__CHAIN_USED]))
                    else (line[self.__ASSET_TRANSFERED])
                )

                if line[self.__TRANSACTION_TYPE] == self.__DEPOSIT:
                    result.append(
                        IntraTransaction(
                            plugin=self.__ROBINHOOD_PLUGIN,
                            unique_id=line[self.__TXN_ID],
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=asset,
                            from_exchange=Keyword.UNKNOWN.value,
                            from_holder=Keyword.UNKNOWN.value,
                            to_exchange=self.__ROBINHOOD,
                            to_holder=self.account_holder,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=Keyword.UNKNOWN.value,
                            crypto_received=str(line[self.__AMOUNT_TRANSFERED]),
                        )
                    )
                elif line[self.__TRANSACTION_TYPE] == self.__WITHDRAWAL:
                    result.append(
                        IntraTransaction(
                            plugin=self.__ROBINHOOD_PLUGIN,
                            unique_id=line[self.__TXN_ID],
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=asset,
                            from_exchange=self.__ROBINHOOD,
                            from_holder=self.account_holder,
                            to_exchange=Keyword.UNKNOWN.value,
                            to_holder=Keyword.UNKNOWN.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=str(line[self.__AMOUNT_TRANSFERED]),
                            crypto_received=Keyword.UNKNOWN.value,
                        )
                    )
                else:
                    self.__logger.error("Unrecognized Crypto transfer: %s", raw_data)

            return result
