import dataclasses
from typing import List, Dict, Optional, Any

# Default prefixes for IDs
NODE_ACTOR_PREFIX = "htcaid:node;"
LINK_ACTOR_PREFIX = "htcaid:link;"
CAR_ACTOR_PREFIX = "htcaid:car;"

NODE_RESOURCE_PREFIX = "htcrid:node;"
LINK_RESOURCE_PREFIX = "htcrid:link;"
CAR_RESOURCE_PREFIX = "htcrid:car;"

NODE_CLASS_TYPE = "mobility.actor.Node"
LINK_CLASS_TYPE = "mobility.actor.Link"
CAR_CLASS_TYPE = "mobility.actor.Car"
NODE_STATE_TYPE = "mobility.entity.state.NodeState"
LINK_STATE_TYPE = "model.mobility.entity.state.LinkState"
CAR_STATE_TYPE = "model.mobility.entity.state.CarState"

# Intermediate structures (after XML parsing)
@dataclasses.dataclass
class RawNode:
    id: str
    x: str
    y: str

@dataclasses.dataclass
class RawLinkAttribute:
    name: str
    value: str

@dataclasses.dataclass
class RawLink:
    id: str
    from_node: str
    to_node: str
    length: str
    freespeed: str
    capacity: str
    permlanes: str
    oneway: str
    modes: str
    attributes: List[RawLinkAttribute] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class RawTrip:
    name: str # Used as the car ID
    origin_node: str
    destination_node: str
    link_origin: str
    count: str # Not used in the final JSON, but may be useful
    start_time: str
    mode: str
    # digital_rails_capable: str # Not used in the final JSON

# Final structures (for JSON)
@dataclasses.dataclass
class DependencyInfo:
    id: str
    resourceId: str
    classType: str
    actorType: str = "LoadBalancedDistributed"

@dataclasses.dataclass
class NodeContent:
    startTick: int = 0
    latitude: str = "" # Mapped from 'x'
    longitude: str = "" # Mapped from 'y'
    scheduleOnTimeManager: bool = False

@dataclasses.dataclass
class NodeData:
    dataType: str = NODE_STATE_TYPE
    content: NodeContent = dataclasses.field(default_factory=NodeContent)

@dataclasses.dataclass
class NodeActor:
    id: str
    name: str
    typeActor: str = NODE_CLASS_TYPE
    data: NodeData = dataclasses.field(default_factory=NodeData)
    dependencies: Dict = dataclasses.field(default_factory=dict)
    # Helper field to know in which file to save
    resource_id: Optional[str] = None

@dataclasses.dataclass
class LinkContent:
    startTick: int = 0
    from_node: str = "" # Will be filled with the node actor ID
    to_node: str = ""   # Will be filled with the node actor ID
    capperiod: Optional[str] = None
    effectivecellsize: Optional[float] = None
    effectivelanewidth: Optional[float] = None
    length: float = 0.0
    lanes: int = 1 # Derived from permlanes
    freeSpeed: float = 0.0
    capacity: float = 0.0
    permlanes: float = 0.0
    modes: List[str] = dataclasses.field(default_factory=list)
    linkType: Optional[str] = None
    scheduleOnTimeManager: bool = False

@dataclasses.dataclass
class LinkData:
    dataType: str = LINK_STATE_TYPE
    content: LinkContent = dataclasses.field(default_factory=LinkContent)

@dataclasses.dataclass
class LinkDependencies:
    from_node: Optional[DependencyInfo] = None
    to_node: Optional[DependencyInfo] = None

@dataclasses.dataclass
class LinkActor:
    id: str
    name: str
    typeActor: str = LINK_CLASS_TYPE
    data: LinkData = dataclasses.field(default_factory=LinkData)
    dependencies: LinkDependencies = dataclasses.field(default_factory=LinkDependencies)
    # Helper field to know in which file to save
    resource_id: Optional[str] = None

@dataclasses.dataclass
class CarContent:
    startTick: int = 0
    origin: str = "" # Will be filled with the node actor ID
    destination: str = "" # Will be filled with the node actor ID
    linkOrigin: str = "" # Will be filled with the link actor ID
    gpsId: str = "htcaid:gps;1" # Will be filled with the GPS resource ID
    scheduleOnTimeManager: bool = True

@dataclasses.dataclass
class CarData:
    dataType: str = CAR_STATE_TYPE
    content: CarContent = dataclasses.field(default_factory=CarContent)

@dataclasses.dataclass
class CarDependencies: # Following the example, depends on origin/destination nodes
    from_node: Optional[DependencyInfo] = None # Represents the origin node
    to_node: Optional[DependencyInfo] = None   # Represents the destination node
    gps: DependencyInfo = dataclasses.field(default_factory=lambda: DependencyInfo(
        id="htcaid:gps;1",
        resourceId="htcrid:gps;1",
        classType="mobility.actor.GPS",
        actorType="PoolDistributed"
    )) # Represents the GPS resource

@dataclasses.dataclass
class CarActor:
    id: str # Generated from the trip name
    name: str # Name based on the origin node (following example)
    typeActor: str = CAR_CLASS_TYPE
    data: CarData = dataclasses.field(default_factory=CarData)
    dependencies: CarDependencies = dataclasses.field(default_factory=CarDependencies)
    # Helper field to know in which file to save
    resource_id: Optional[str] = None

# To store global attributes of links
@dataclasses.dataclass
class GlobalLinkAttributes:
    capperiod: Optional[str] = None
    effectivecellsize: Optional[float] = None
    effectivelanewidth: Optional[float] = None

# For the configuration file
@dataclasses.dataclass
class DataSourceInfo:
    path: str

@dataclasses.dataclass
class DataSource:
    sourceType: str = "json"
    info: DataSourceInfo = dataclasses.field(default_factory=DataSourceInfo)

@dataclasses.dataclass
class ActorDataSource:
    id: str # Resource ID
    classType: str
    dataSource: DataSource = dataclasses.field(default_factory=DataSource)

@dataclasses.dataclass
class SimulationConfig:
    name: str
    description: str
    startRealTime: str
    timeUnit: str
    timeStep: int
    duration: int
    startTick: int
    actorsDataSources: List[ActorDataSource] = dataclasses.field(default_factory=list)

# Helper to convert dataclasses into dicts (removing None and auxiliary fields)
def asdict_factory(data):
    def convert_value(obj):
        if isinstance(obj, list):
            return [convert_value(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: convert_value(v) for k, v in obj.items() if v is not None and k != 'resource_id'}
        elif dataclasses.is_dataclass(obj):
            # Ignore the auxiliary field resource_id during final serialization
            return {k: convert_value(v) for k, v in dataclasses.asdict(obj).items() if v is not None and k != 'resource_id'}
        else:
            return obj
    # Also filter Nones at the root level and the resource_id field
    return {k: convert_value(v) for k, v in data if v is not None and k != 'resource_id'}

def to_dict(obj):
    """Converts a dataclass to a dictionary, recursively removing Nones."""
    return dataclasses.asdict(obj, dict_factory=asdict_factory)
