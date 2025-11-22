"""
Centralized constants for the CLS Allscripts Pipeline.
Use these constants instead of hardcoded strings in your code.
"""

class Columns:
    """Column names used across the pipeline"""
    # Key Identity Columns
    PMM_ITEM_NUMBER = "PMM Item Number"
    CORP_ACCT = "Corp Acct"
    VENDOR_CODE = "Vendor Code"
    ADD_COST_CENTRE = "Additional Cost Centre"
    ADD_GL_ACCOUNT = "Additional GL Account"
    
    # Date Columns
    DATE_AND_TIME = "Date and Time Stamp"
    CONTRACT_START = "Contract Start Date"
    CONTRACT_END = "Contract End Date"
    ITEM_UPDATE_DATE = "Item Update Date"
    
    # Numeric/Price Columns
    UOM1_QTY = "UOM1 QTY"
    UOM2_QTY = "UOM2 QTY"
    UOM3_QTY = "UOM3 QTY"
    PURCHASE_UOM_PRICE = "Purchase UOM Price"
    PRICE_1 = "Price1"
    
    # Metadata
    SOURCE_FILE = "Source_File"
    INDEX = "Index"

class FilePatterns:
    """File naming patterns"""
    DAILY_INCREMENTAL = "0031-Contract Item Price Cat Pkg Extract *.xlsx"
    WEEKLY_FULL = "0031-Contract Item Price Cat Pkg Extract [0-9][0-9][0-9][0-9].xlsx"
    DATABASE_PARQUET = "0031.parquet"
