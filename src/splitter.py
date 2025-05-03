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

# --- Mapeamento e Conversão ---

def map_raw_to_node_actor(raw_node: RawNode) -> NodeActor:
    """Converte RawNode para NodeActor (sem resource_id ainda)."""
    actor_id = generate_actor_id(NODE_ACTOR_PREFIX, raw_node.id)
    return NodeActor(
        id=actor_id,
        name=f"Node{raw_node.id}",
        data=NodeData(content=NodeContent(latitude=raw_node.x, longitude=raw_node.y))
    )

def map_raw_to_link_actor(
    raw_link: RawLink,
    global_attrs: GlobalLinkAttributes,
    node_map: Dict[str, NodeActor], # Mapa de original_node_id -> NodeActor (com resource_id)
    link_actor_id: str,
    link_resource_id: str # Recurso deste próprio link
) -> LinkActor:
    """Converte RawLink para LinkActor, resolvendo dependências."""
    link_type = next((attr.value for attr in raw_link.attributes if attr.name == 'type'), None)
    modes = [mode.strip() for mode in raw_link.modes.split(',') if mode.strip()]

    from_node_actor = node_map.get(raw_link.from_node)
    to_node_actor = node_map.get(raw_link.to_node)

    if not from_node_actor or not from_node_actor.resource_id:
        logger.warning(f"Nó de origem '{raw_link.from_node}' não encontrado ou sem resource_id para o link '{raw_link.id}'. Dependência ficará incompleta.")
    if not to_node_actor or not to_node_actor.resource_id:
        logger.warning(f"Nó de destino '{raw_link.to_node}' não encontrado ou sem resource_id para o link '{raw_link.id}'. Dependência ficará incompleta.")

    # Criação das dependências
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

    # Tratamento de tipos e valores padrão
    try: length = float(raw_link.length)
    except (ValueError, TypeError): length = 0.0; logger.warning(f"Link {raw_link.id}: Comprimento inválido '{raw_link.length}', usando 0.0")
    try: free_speed = float(raw_link.freespeed)
    except (ValueError, TypeError): free_speed = 0.0; logger.warning(f"Link {raw_link.id}: Velocidade livre inválida '{raw_link.freespeed}', usando 0.0")
    try: capacity = float(raw_link.capacity)
    except (ValueError, TypeError): capacity = 0.0; logger.warning(f"Link {raw_link.id}: Capacidade inválida '{raw_link.capacity}', usando 0.0")
    try: permlanes = float(raw_link.permlanes)
    except (ValueError, TypeError): permlanes = 1.0; logger.warning(f"Link {raw_link.id}: Permlanes inválido '{raw_link.permlanes}', usando 1.0")
    try: lanes = int(permlanes)
    except (ValueError, TypeError): lanes = 1; logger.warning(f"Link {raw_link.id}: Não foi possível converter permlanes '{permlanes}' para int, usando 1")


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
        name=f"Client{raw_link.id}", # Follow example
        data=LinkData(content=content),
        dependencies=dependencies,
        resource_id=link_resource_id # Add the resource_id to the linst actor itself
    )

def map_raw_to_car_actor(
    raw_trip: RawTrip,
    node_map: Dict[str, NodeActor], # Mapa de original_node_id -> NodeActor (com resource_id)
    link_map: Dict[str, LinkActor], # Mapa de original_link_id -> LinkActor (com resource_id)
    car_actor_id: str,
    car_resource_id: str
) -> CarActor:
    """Converte RawTrip para CarActor, resolvendo dependências."""
    origin_node_actor = node_map.get(raw_trip.origin_node)
    destination_node_actor = node_map.get(raw_trip.destination_node)
    origin_link_actor = link_map.get(raw_trip.link_origin)

    if not origin_node_actor or not origin_node_actor.resource_id:
        logger.warning(f"Nó de origem '{raw_trip.origin_node}' não encontrado ou sem resource_id para a viagem '{raw_trip.name}'. Dependência ficará incompleta.")
    if not destination_node_actor or not destination_node_actor.resource_id:
        logger.warning(f"Nó de destino '{raw_trip.destination_node}' não encontrado ou sem resource_id para a viagem '{raw_trip.name}'. Dependência ficará incompleta.")
    if not origin_link_actor:
         logger.warning(f"Link de origem '{raw_trip.link_origin}' não encontrado para a viagem '{raw_trip.name}'. Campo linkOrigin ficará incompleto.")

    # Criação das dependências (baseado no exemplo, apenas nós)
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
        start_tick = int(float(raw_trip.start_time)) # MATSim pode usar float para tempo
    except (ValueError, TypeError):
        logger.warning(f"Tempo de início inválido '{raw_trip.start_time}' para viagem {raw_trip.name}, usando 0.")
        start_tick = 0

    content = CarContent(
        startTick=start_tick,
        origin=origin_node_actor.id if origin_node_actor else f"MISSING_NODE_{raw_trip.origin_node}",
        destination=destination_node_actor.id if destination_node_actor else f"MISSING_NODE_{raw_trip.destination_node}",
        linkOrigin=origin_link_actor.id if origin_link_actor else f"MISSING_LINK_{raw_trip.link_origin}"
    )

    # Nome do Carro segue o exemplo estranho: Node<origin_id>
    car_name = f"Node{raw_trip.origin_node}"

    return CarActor(
        id=car_actor_id,
        name=car_name, # Usando a convenção do exemplo
        data=CarData(content=content),
        dependencies=dependencies,
        resource_id=car_resource_id # Adiciona o resource_id ao próprio ator do carro
    )


# --- Divisão e Salvamento ---

def assign_resource_ids(items: List[Any], max_per_file: int, resource_prefix: str) -> Tuple[Dict[str, str], List[Any]]:
    """
    Atribui resource_id a cada item (NodeActor, LinkActor, CarActor)
    e retorna um mapa de original_id -> resource_id e a lista de atores atualizada.

    Assume que os itens já possuem um campo 'id' (actor_id) e um campo para o ID original
    (implícito no mapeamento que será feito a partir do actor_id).
    Precisa modificar os itens para adicionar o campo 'resource_id'.
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
        item.resource_id = resource_id # Modifica o objeto ator diretamente

        # Extrai o ID original do ID do ator para o mapa
        # Ex: "dtmi:...:node;1001" -> "1001"
        original_id = item.id.split(';')[-1]
        id_to_resource_map[original_id] = resource_id
        updated_items.append(item)
        item_count += 1

    return id_to_resource_map, updated_items


def split_and_save(
    actors: List[Any], # Lista de NodeActor, LinkActor ou CarActor com resource_id preenchido
    base_filename: str, # "nodes", "links", "cars"
    output_dir: Path,
    pretty: bool,
    use_gzip: bool
) -> List[Dict[str, str]]:
    """
    Divide a lista de atores com base no resource_id e salva em arquivos JSON.
    Retorna uma lista de dicionários com informações sobre os arquivos gerados.
    """
    logger.info(f"Iniciando divisão e salvamento para: {base_filename}")
    files_info = []
    # Agrupa atores pelo resource_id atribuído
    grouped_actors: Dict[str, List[Any]] = {}
    for actor in actors:
        if actor.resource_id:
            if actor.resource_id not in grouped_actors:
                grouped_actors[actor.resource_id] = []
            grouped_actors[actor.resource_id].append(actor)
        else:
            logger.warning(f"Ator {actor.id} sem resource_id, não será salvo.")

    # Ordena pelos resource_ids para garantir a numeração correta dos arquivos (ex: node;1, node;2)
    sorted_resource_ids = sorted(grouped_actors.keys(), key=lambda x: int(x.split(';')[-1]))

    for resource_id in sorted_resource_ids:
        chunk = grouped_actors[resource_id]
        file_index = resource_id.split(';')[-1]
        filename = f"{base_filename}_{file_index}"
        filepath = output_dir / filename
        # Converte para dict antes de salvar, removendo campos nulos/auxiliares
        data_to_save = [to_dict(actor) for actor in chunk]
        try:
            save_json(data_to_save, filepath, pretty, use_gzip)
            final_filename = f"{filename}{'.json.gz' if use_gzip else '.json'}"
            files_info.append({"resource_id": resource_id, "filename": final_filename})
            logger.info(f"Salvo arquivo {final_filename} com {len(chunk)} atores.")
        except Exception as e:
            logger.error(f"Falha ao salvar o arquivo {filename}: {e}")
            # Decide se quer parar ou continuar
            # raise

    logger.info(f"Finalizado salvamento para {base_filename}. Gerados {len(files_info)} arquivos.")
    return files_info