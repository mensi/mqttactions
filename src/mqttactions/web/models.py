from typing import List, Optional
from pydantic import BaseModel


class Node(BaseModel):
    id: str
    label: str
    type: str = "state"


class Edge(BaseModel):
    id: str
    source: str
    target: str
    type: str
    label: str
    topic: Optional[str] = None
    filter: Optional[str] = None
    timeout: Optional[float] = None


class Diagram(BaseModel):
    nodes: List[Node]
    edges: List[Edge]
    currentState: Optional[str] = None


class StateMachineData(BaseModel):
    name: str
    currentState: Optional[str]
    diagram: Diagram


class InitialData(BaseModel):
    topics: List[str]
    statemachines: List[StateMachineData]


class InjectMessage(BaseModel):
    topic: str
    payload: str
