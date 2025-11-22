"""
Centralized constants for the CLS Allscripts Pipeline.
Use these constants instead of hardcoded strings in your code.
"""


class DailyColumns:
    """Column names for Daily Files (System A)"""

    DISTRIBUTOR_PART_NUMBER = 'Distributor Part Number'
    MANUFACTURER_PART_NUMBER = 'Manufacturer Part Number'
    PMM = 'PMM'
    PLANT_ID = 'Plant ID'
    DISTRIBUTOR = 'Distributor'
    SOURCE_FILE = 'Source_File'


class Columns0031:
    """Column names used across the pipeline (System B - 0031)"""

    # Key Identity Columns
    PMM_ITEM_NUMBER = 'PMM Item Number'
    CORP_ACCT = 'Corp Acct'
    VENDOR_CODE = 'Vendor Code'
    ADD_COST_CENTRE = 'Additional Cost Centre'
    ADD_GL_ACCOUNT = 'Additional GL Account'

    # Date Columns
    DATE_AND_TIME = 'Date and Time Stamp'
    CONTRACT_START = 'Contract Start Date'
    CONTRACT_END = 'Contract End Date'
    ITEM_UPDATE_DATE = 'Item Update Date'
    ITEM_CREATE_DATE = 'Item Create Date'

    # Numeric/Price Columns
    UOM1_QTY = 'UOM1 QTY'
    UOM2_QTY = 'UOM2 QTY'
    UOM3_QTY = 'UOM3 QTY'
    PURCHASE_UOM_PRICE = 'Purchase UOM Price'
    PRICE_1 = 'Price1'
    PRICE_2 = 'Price2'
    PRICE_3 = 'Price3'
    DEFAULT_UOM_PRICE = 'Default UOM Price'

    # Metadata
    SOURCE_FILE = 'Source_File'
    INDEX = 'Index'

    # Contract Columns
    CONTRACT_NO = 'Contract No'
    CONTRACT_EFF_DATE = 'Contract EFF Date'
    CONTRACT_EXP_DATE = 'Contract EXP Date'
    CONTRACT_ITEM = 'Contract Item'
    VENDOR_NAME = 'Vendor Name'
    VENDOR_SEQ = 'Vendor Seq'
    ITEM_DESCRIPTION = 'Item Description'
    MANUFACTURER_CATALOGUE = 'Manufacturer Catalogue'
    VENDOR_CATALOGUE = 'Vendor Catalogue'


class FilePatterns:
    """File naming patterns"""

    DAILY_INCREMENTAL = '0031-Contract Item Price Cat Pkg Extract *.xlsx'
    WEEKLY_FULL = '0031-Contract Item Price Cat Pkg Extract [0-9][0-9][0-9][0-9].xlsx'
    DATABASE_PARQUET = '0031.parquet'


import polars as pl

class Schema0031:
    """Expected schema for 0031 database"""
    
    SCHEMA = {
        Columns0031.PMM_ITEM_NUMBER: pl.Utf8,
        Columns0031.CORP_ACCT: pl.Utf8,
        Columns0031.VENDOR_CODE: pl.Utf8,
        Columns0031.ADD_COST_CENTRE: pl.Utf8,
        Columns0031.ADD_GL_ACCOUNT: pl.Utf8,
        
        # Dates
        Columns0031.DATE_AND_TIME: pl.Date,
        Columns0031.CONTRACT_START: pl.Date,
        Columns0031.CONTRACT_END: pl.Date,
        Columns0031.ITEM_UPDATE_DATE: pl.Date,
        Columns0031.CONTRACT_EFF_DATE: pl.Date,
        Columns0031.CONTRACT_EXP_DATE: pl.Date,
        Columns0031.ITEM_CREATE_DATE: pl.Date,
        
        # Numeric
        Columns0031.UOM1_QTY: pl.Float32,
        Columns0031.UOM2_QTY: pl.Float32,
        Columns0031.UOM3_QTY: pl.Float32,
        Columns0031.PURCHASE_UOM_PRICE: pl.Float32,
        Columns0031.PRICE_1: pl.Float32,
        Columns0031.PRICE_2: pl.Float32,
        Columns0031.PRICE_3: pl.Float32,
        Columns0031.DEFAULT_UOM_PRICE: pl.Float32,
        
        # Strings
        Columns0031.CONTRACT_NO: pl.Utf8,
        Columns0031.CONTRACT_ITEM: pl.Utf8,
        Columns0031.VENDOR_NAME: pl.Utf8,
        Columns0031.VENDOR_SEQ: pl.Utf8,
        Columns0031.ITEM_DESCRIPTION: pl.Utf8,
        Columns0031.MANUFACTURER_CATALOGUE: pl.Utf8,
        Columns0031.VENDOR_CATALOGUE: pl.Utf8,
    }
