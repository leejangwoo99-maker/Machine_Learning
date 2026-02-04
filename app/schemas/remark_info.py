from __future__ import annotations
from pydantic import BaseModel, Field, field_validator
import string


class RemarkInfoIn(BaseModel):
    barcode_information: str = Field(..., description="length 1, printable")
    pn: str | None = None
    remark: str | None = None

    @field_validator("barcode_information")
    @classmethod
    def validate_barcode_information(cls, v: str) -> str:
        if v is None or len(v) != 1:
            raise ValueError("barcode_information must be length 1")
        if v not in string.printable:
            raise ValueError("barcode_information must be printable")
        return v  # case-preserve


class RemarkInfoOut(BaseModel):
    barcode_information: str
    pn: str | None = None
    remark: str | None = None
