from pydantic import BaseModel, RootModel, Field
from typing import List, Dict, Any, Optional
from enum import Enum

class RunMode(str, Enum):
    yes = "yes"
    never = "never"
    main_only = "main_only"
    sub_only = "sub_only"


class MtaskModel(BaseModel):
    ### Required
    name: str
    action: str

    ### Optional
    args: Dict[str, Any] = {}
    run: RunMode = RunMode.yes

class ArgsModel(BaseModel):
    in_: List[str] = Field(default_factory=list, alias='in')
    out: List[str] = Field(default_factory=list)
    in1: str = ''
    in2: str = ''
    group: str = ''
    frame_group_name: str = ''
    frame_group_name_in: str = ''
    frame_group_name_out: str = ''


class ProvideItem(BaseModel):
    key: str
    type: str

#class ProvidesModel(RootModel):
#    root: Dict[str, ProvideItem]

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
    # Optional, defaults to yes
    run: RunMode = RunMode.yes

    # (opt.) required (task)resources,
    # either in context #
    req_resources: List[str] = []
    # (opt.) required tasks that must be run before this one
    req_tasks: List[str] = []

    # (opt.) Resources provided if this tasks was run
    # these keys are the same for context and for framecache
    provides: Dict[str, ProvideItem] = {}
    #provides: Dict[str, ProvidesModel] = {}
    # if a dict of results is provided, e.g. when using loop_items

    frame_group: str = ''

    loop_items: List[Any] = []



class PipelineModel(BaseModel):
    out: Dict[str, Any]
    tasks: List[TaskModel]
