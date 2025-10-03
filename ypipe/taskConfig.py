from pydantic import BaseModel
from typing import List, Dict, Any


class TaskModel(BaseModel):
    # name can have spaces but is the ID
    name: str
    # action is required, 
    action: str

    # (opt.) required (task)resources
    req: List[str] = []
    # (opt.) Resources provided if this tasks was run
    provides: List[str] = []
    # (opt.)
    args: Dict[str, Any] = {}


class PipelineModel(BaseModel):
    out: Dict[str, Any]
    tasks: List[TaskModel]
