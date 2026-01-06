from pydantic import BaseModel
from typing import Any, Dict, List, Optional

class HealthResp(BaseModel):
    status: str

class DbPingResp(BaseModel):
    db: str
    select_1: int

class VisionPassFailResp(BaseModel):
    prod_day: str
    day: Dict[str, Any]
    night: Dict[str, Any]

class YieldResp(BaseModel):
    prod_day: str
    day: Dict[str, Any]
    night: Dict[str, Any]

class FailAnalysisResp(BaseModel):
    prod_day: str
    day: Dict[str, Any]
    night: Dict[str, Any]

class OeeResp(BaseModel):
    prod_day: str
    day: Dict[str, Any]
    night: Dict[str, Any]

class MasterSampleResp(BaseModel):
    prod_day: str
    now: str
    mastersample_ok: bool
