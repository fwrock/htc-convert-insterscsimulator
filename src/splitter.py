import logging
import math
from pathlib import Path
from typing import List, Dict, Tuple, Any
from .models import (
    RawNode, RawLink, RawTrip, GlobalLinkAttributes,
    NodeActor, NodeContent, NodeData,
    LinkActor, LinkContent, LinkData, LinkDependencies, DependencyInfo,
    CarActor, CarContent, CarData, CarDependencies,
    NODE_ACTOR_PREFIX, LINK_ACTOR_PREFIX, CAR_ACTOR_PREFIX,
    NODE_RESOURCE_PREFIX, LINK_RESOURCE_PREFIX, CAR_RESOURCE_PREFIX,
    NODE_CLASS_TYPE, LINK_CLASS_TYPE, CAR_CLASS_TYPE,
    to_dict
)
from .utils import save_json, chunk_list, generate_resource_id, generate_actor_id, logger


# --- Mapping and Conversion ---

def map_raw_to_node_actor(raw_node: RawNode) -> NodeActor:
    """Converts RawNode to NodeActor (without resource_id yet)."""
    actor_id = generate_actor_id(NODE_ACTOR_PREFIX, raw_node.id)
    return NodeActor(
        id=actor_id,
        name=f"Node{raw_node.id}",
        data=NodeData(content=NodeContent(latitude=raw_node.x, longitude=raw_node.y))
    )

def map_raw_to_link_actor(
    raw_link: RawLink,
    global_attrs: GlobalLinkAttributes,
    node_map: Dict[str, NodeActor], # Map of original_node_id -> NodeActor (with resource_id)
    link_actor_id: str,
    link_resource_id: str # Resource ID of this link itself
) -> LinkActor:
    """Converts RawLink to LinkActor, resolving dependencies."""
    link_type = next((attr.value for attr in raw_link.attributes if attr.name == 'type'), None)
    modes = [mode.strip() for mode in raw_link.modes.split(',') if mode.strip()]

    from_node_actor = node_map.get(raw_link.from_node)
    to_node_actor = node_map.get(raw_link.to_node)

    if not from_node_actor or not from_node_actor.resource_id:
        logger.warning(f"Origin node '{raw_link.from_node}' not found or missing resource_id for link '{raw_link.id}'. Dependency will be incomplete.")
    if not to_node_actor or not to_node_actor.resource_id:
        logger.warning(f"Destination node '{raw_link.to_node}' not found or missing resource_id for link '{raw_link.id}'. Dependency will be incomplete.")

    # Create dependencies
    dependencies = LinkDependencies(
        from_node=DependencyInfo(
            id=from_node_actor.id,
            resourceId=from_node_actor.resource_id,
            classType=NODE_CLASS_TYPE
        ) if from_node_actor and from_node_actor.resource_id else None,
        to_node=DependencyInfo(
            id=to_node_actor.id,
            resourceId=to_node_actor.resource_id,
            classType=NODE_CLASS_TYPE
        ) if to_node_actor and to_node_actor.resource_id else None
    )

    # Handling types and default values
    try: length = float(raw_link.length)
    except (ValueError, TypeError): length = 0.0; logger.warning(f"Link {raw_link.id}: Invalid length '{raw_link.length}', using 0.0")
    try: free_speed = float(raw_link.freespeed)
    except (ValueError, TypeError): free_speed = 0.0; logger.warning(f"Link {raw_link.id}: Invalid free speed '{raw_link.freespeed}', using 0.0")
    try: capacity = float(raw_link.capacity)
    except (ValueError, TypeError): capacity = 0.0; logger.warning(f"Link {raw_link.id}: Invalid capacity '{raw_link.capacity}', using 0.0")
    try: permlanes = float(raw_link.permlanes)
    except (ValueError, TypeError): permlanes = 1.0; logger.warning(f"Link {raw_link.id}: Invalid permlanes '{raw_link.permlanes}', using 1.0")
    try: lanes = int(permlanes)
    except (ValueError, TypeError): lanes = 1; logger.warning(f"Link {raw_link.id}: Failed to convert permlanes '{permlanes}' to int, using 1")

    content = LinkContent(
        from_node=from_node_actor.id if from_node_actor else f"MISSING_NODE_{raw_link.from_node}",
        to_node=to_node_actor.id if to_node_actor else f"MISSING_NODE_{raw_link.to_node}",
        capperiod=global_attrs.capperiod,
        effectivecellsize=global_attrs.effectivecellsize,
        effectivelanewidth=global_attrs.effectivelanewidth,
        length=length,
        lanes=lanes,
        freeSpeed=free_speed,
        capacity=capacity,
        permlanes=permlanes,
        modes=modes,
        linkType=link_type
    )

    return LinkActor(
        id=link_actor_id,
        name=f"Client{raw_link.id}", # As per example
        data=LinkData(content=content),
        dependencies=dependencies,
        resource_id=link_resource_id # Adds the resource_id to the link actor itself
    )

def map_raw_to_car_actor(
    raw_trip: RawTrip,
    node_map: Dict[str, NodeActor], # Map of original_node_id -> NodeActor (with resource_id)
    link_map: Dict[str, LinkActor], # Map of original_link_id -> LinkActor (with resource_id)
    car_actor_id: str,
    car_resource_id: str
) -> CarActor:
    """Converts RawTrip to CarActor, resolving dependencies."""
    origin_node_actor = node_map.get(raw_trip.origin_node)
    destination_node_actor = node_map.get(raw_trip.destination_node)
    origin_link_actor = link_map.get(raw_trip.link_origin)

    if not origin_node_actor or not origin_node_actor.resource_id:
        logger.warning(f"Origin node '{raw_trip.origin_node}' not found or missing resource_id for trip '{raw_trip.name}'. Dependency will be incomplete.")
    if not destination_node_actor or not destination_node_actor.resource_id:
        logger.warning(f"Destination node '{raw_trip.destination_node}' not found or missing resource_id for trip '{raw_trip.name}'. Dependency will be incomplete.")
    if not origin_link_actor:
         logger.warning(f"Origin link '{raw_trip.link_origin}' not found for trip '{raw_trip.name}'. linkOrigin field will be incomplete.")

    # Create dependencies (based on the example, only nodes)
    dependencies = CarDependencies(
         from_node=DependencyInfo(
            id=origin_node_actor.id,
            resourceId=origin_node_actor.resource_id,
            classType=NODE_CLASS_TYPE
        ) if origin_node_actor and origin_node_actor.resource_id else None,
        to_node=DependencyInfo(
            id=destination_node_actor.id,
            resourceId=destination_node_actor.resource_id,
            classType=NODE_CLASS_TYPE
        ) if destination_node_actor and destination_node_actor.resource_id else None
    )

    try:
        start_tick = int(float(raw_trip.start_time)) # MATSim may use float for time
    except (ValueError, TypeError):
        logger.warning(f"Invalid start time '{raw_trip.start_time}' for trip {raw_trip.name}, using 0.")
        start_tick = 0

    content = CarContent(
        startTick=start_tick,
        origin=origin_node_actor.id if origin_node_actor else f"MISSING_NODE_{raw_trip.origin_node}",
        destination=destination_node_actor.id if destination_node_actor else f"MISSING_NODE_{raw_trip.destination_node}",
        linkOrigin=origin_link_actor.id if origin_link_actor else f"MISSING_LINK_{raw_trip.link_origin}"
    )

    # Car name follows the strange example: Node<origin_id>
    car_name = f"Node{raw_trip.origin_node}"

    return CarActor(
        id=car_actor_id,
        name=car_name, # Following the convention from the example
        data=CarData(content=content),
        dependencies=dependencies,
        resource_id=car_resource_id # Adds the resource_id to the car actor itself
    )


# --- Splitting and Saving ---

def assign_resource_ids(items: List[Any], max_per_file: int, resource_prefix: str) -> Tuple[Dict[str, str], List[Any]]:
    """
    Assigns a resource_id to each item (NodeActor, LinkActor, CarActor)
    and returns a map of original_id -> resource_id and the updated list of actors.

    Assumes the items already have an 'id' field (actor_id) and an implicit field for the original ID
    (extracted from actor_id during mapping).
    Needs to modify the items to add the 'resource_id' field.
    """
    id_to_resource_map: Dict[str, str] = {}
    updated_items: List[Any] = []
    item_count = 0
    file_index = 1

    for item in items:
        if item_count >= max_per_file:
            file_index += 1
            item_count = 0

        resource_id = generate_resource_id(resource_prefix, file_index)
        item.resource_id = resource_id # Modifies the actor object directly

        # Extracts the original ID from the actor's ID for the map
        # Ex: "dtmi:...:node;1001" -> "1001"
        original_id = item.id.split(';')[-1]
        id_to_resource_map[original_id] = resource_id
        updated_items.append(item)
        item_count += 1

    return id_to_resource_map, updated_items


def split_and_save(
    actors: List[Any], # List of NodeActor, LinkActor or CarActor with filled resource_id
    base_filename: str, # "nodes", "links", "cars"
    output_dir: Path,
    pretty: bool,
    use_gzip: bool
) -> List[Dict[str, str]]:
    """
    Splits the list of actors based on resource_id and saves them into JSON files.
    Returns a list of dictionaries with information about the generated files.
    """
    logger.info(f"Starting split and save for: {base_filename}")
    files_info = []
    # Group actors by their assigned resource_id
    grouped_actors: Dict[str, List[Any]] = {}
    for actor in actors:
        if actor.resource_id:
            if actor.resource_id not in grouped_actors:
                grouped_actors[actor.resource_id] = []
            grouped_actors[actor.resource_id].append(actor)
        else:
            logger.warning(f"Actor {actor.id} missing resource_id, will not be saved.")

    # Sort by resource_ids to ensure correct file numbering (e.g., node;1, node;2)
    sorted_resource_ids = sorted(grouped_actors.keys(), key=lambda x: int(x.split(';')[-1]))

    for resource_id in sorted_resource_ids:
        chunk = grouped_actors[resource_id]
        file_index = resource_id.split(';')[-1]
