import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from .models import SimulationConfig, ActorDataSource, DataSource, DataSourceInfo, to_dict, \
                    NODE_CLASS_TYPE, LINK_CLASS_TYPE, CAR_CLASS_TYPE
from .utils import save_json, logger

def generate_simulation_config(
    scenario_name: str,
    start_real_time: str,
    duration: int,
    time_unit: str,
    time_step: int,
    start_tick: int,
    node_files: List[Dict[str, str]],
    link_files: List[Dict[str, str]],
    car_files: List[Dict[str, str]],
    output_dir: Path,
    pretty: bool,
    use_gzip: bool # simulation.json configuration is not affected by gzip
):
    """Generates the simulation.json configuration file."""
    logger.info("Generating simulation.json configuration file")

    config = SimulationConfig(
        name=f"HTC-Simulator: {scenario_name}",
        description="Simulates a smart mobility scenario with a map and car trips generated from MATSim data",
        startRealTime=start_real_time,
        timeUnit=time_unit,
        timeStep=time_step,
        duration=duration,
        startTick=start_tick,
        actorsDataSources=[]
    )

    base_path = f"/app/hyperbolic-time-chamber/simulations/input/{scenario_name}"

    # Add data sources for nodes
    for file_info in node_files:
        resource_id = file_info['resource_id']
        filename = file_info['filename']
        config.actorsDataSources.append(
            ActorDataSource(
                id=resource_id,
                classType=NODE_CLASS_TYPE,
                dataSource=DataSource(info=DataSourceInfo(path=f"{base_path}/{filename}"))
            )
        )

    # Add data sources for links
    for file_info in link_files:
        resource_id = file_info['resource_id']
        filename = file_info['filename']
        config.actorsDataSources.append(
            ActorDataSource(
                id=resource_id,
                classType=LINK_CLASS_TYPE,
                dataSource=DataSource(info=DataSourceInfo(path=f"{base_path}/{filename}"))
            )
        )

    # Add data sources for cars
    for file_info in car_files:
        resource_id = file_info['resource_id']
        filename = file_info['filename']
        config.actorsDataSources.append(
            ActorDataSource(
                id=resource_id,
                classType=CAR_CLASS_TYPE,
                dataSource=DataSource(info=DataSourceInfo(path=f"{base_path}/{filename}"))
            )
        )

    # Save the configuration file
    config_filepath = output_dir / "simulation.json"
    try:
        # The simulation.json file is usually not compressed
        save_json(to_dict(config), config_filepath, pretty=pretty, use_gzip=False)
        logger.info(f"Configuration file saved at: {config_filepath}")
    except Exception as e:
        logger.error(f"Failed to save the configuration file {config_filepath}: {e}")
        raise
