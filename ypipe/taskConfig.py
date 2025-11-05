from pydantic import BaseModel
from typing import List, Dict, Any


class MtaskModel(BaseModel):
    ### Required
    name: str
    action: str

    ### Optional
    args: Dict[str, Any] = {}
    run: bool = True

class ArgsModel(BaseModel):
    in1: str = ''
    in2: str = ''
    out: str = ''
    group: str = ''
    frame_group: str = ''
    frame_group_dict: bool = False

class TaskModel(BaseModel):
    ### Required
    # task name can have spaces but is the ID
    name: str
    # action term is required, used for task class lookup
    action: str
    # An args dict is always expected. To group the most used params
    #args: Dict[str, Any]
    args: ArgsModel = ArgsModel()


    ### Optional
    # Optional, defaults to true
    run: bool = True
    # (opt.) required (task)resources,
    # either in context #
    req: List[str] = []

    # (opt.) Resources provided if this tasks was run
    # these keys are the same for context and for framecache
    provides: List[str] = []
    # if a dict of results is provided, e.g. when using loop_items
    provided_d: List[str] = []
    provides_dict: bool = False

    frame_group: str = ''

    loop_items: List[Any] = []



class PipelineModel(BaseModel):
    out: Dict[str, Any]
    tasks: List[TaskModel]
