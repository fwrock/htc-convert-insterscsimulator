import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from dateutil import parser as date_parser # Renamed to avoid conflict

from src.parser import parse_network, parse_plans
from src.splitter import (
    map_raw_to_node_actor,
    map_raw_to_link_actor,
    map_raw_to_car_actor,
    assign_resource_ids,
    split_and_save
)
from src.simulation_gen import generate_simulation_config
from src.utils import logger, create_output_dir
from src.models import (
    NODE_ACTOR_PREFIX, LINK_ACTOR_PREFIX, CAR_ACTOR_PREFIX,
    NODE_RESOURCE_PREFIX, LINK_RESOURCE_PREFIX, CAR_RESOURCE_PREFIX
)

def main():
    ap = argparse.ArgumentParser(description="Converts MATSim files (network, plans) to InterSCITY HTC JSON format.")

    # --- Input Files ---
    ap.add_argument("--network", type=Path, required=True, help="Path to network.xml file")
    ap.add_argument("--plans", type=Path, required=True, help="Path to plans.xml or trips.xml file")

    # --- Scenario Settings ---
    ap.add_argument("--scenario-name", type=str, default="smart_mobility", help="Scenario name for organization and configuration.")
    ap.add_argument("--start-real-time", type=str, default=datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
                        help="ISO 8601 timestamp for simulation start (default: now in UTC). E.g.: '2025-01-27T12:30:45.123Z'")
    ap.add_argument("--duration", type=int, default=86400, help="Simulation duration in time units (default: 86400 = 24h in seconds).")
    ap.add_argument("--time-unit", type=str, default="seconds", help="Simulation time unit (default: seconds).")
    ap.add_argument("--time-step", type=int, default=1, help="Simulation time step (default: 1).")
    ap.add_argument("--start-tick", type=int, default=0, help="Simulation starting tick (default: 0).")

    # --- Output and Split Settings ---
    ap.add_argument("--output-dir", type=Path, default=Path("output"), help="Base directory to save the generated files.")
    ap.add_argument("--max-nodes-per-file", type=int, default=1000, help="Maximum number of nodes per JSON file.")
    ap.add_argument("--max-links-per-file", type=int, default=1000, help="Maximum number of links per JSON file.")
    ap.add_argument("--max-trips-per-file", type=int, default=1000, help="Maximum number of trips (cars) per JSON file.")
    ap.add_argument("--gzip", action="store_true", help="Save data files (.json.gz) compressed.")
    ap.add_argument("--pretty", action=argparse.BooleanOptionalAction, default=True, help="Save formatted (indented) JSON. Use --no-pretty to disable.")
    ap.add_argument("-v", "--verbose", action="store_true", help="Increase log level to DEBUG.")

    args = ap.parse_args()

    # Configure logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        for handler in logging.getLogger().handlers:
            handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.info("Logging set to DEBUG.")
    else:
         logging.getLogger().setLevel(logging.INFO)
         for handler in logging.getLogger().handlers:
             handler.setLevel(logging.INFO)
         logger.setLevel(logging.INFO)

    # Validate timestamp
    try:
        parsed_time = date_parser.isoparse(args.start_real_time)
        start_real_time_iso = parsed_time.isoformat(timespec='milliseconds')
        # If no timezone, assume UTC (or local, but UTC is safer)
        if parsed_time.tzinfo is None:
             logger.warning(f"Timestamp '{args.start_real_time}' has no timezone. Assuming UTC.")
             start_real_time_iso = datetime.fromisoformat(args.start_real_time).replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds')

        logger.info(f"Using StartRealTime: {start_real_time_iso}")
    except ValueError:
        logger.error(f"Invalid format for --start-real-time: '{args.start_real_time}'. Use ISO 8601 (e.g.: 2025-01-27T12:30:45.123Z).")
        exit(1)

    # Create specific output directory for the scenario
    scenario_output_dir = args.output_dir / args.scenario_name
    create_output_dir(scenario_output_dir)

    # --- Step 1: Parse XML ---
    if not args.network.is_file(): logger.error(f"Network file not found: {args.network}"); exit(1)
    if not args.plans.is_file(): logger.error(f"Plans file not found: {args.plans}"); exit(1)

    raw_nodes, raw_links, global_link_attrs = parse_network(args.network)
    raw_trips = parse_plans(args.plans)

    if not raw_nodes: logger.warning("No nodes found in network file."); # Can continue, but generating links/cars will fail
    if not raw_links: logger.warning("No links found in network file.");
    if not raw_trips: logger.warning("No car trips found in plans file.");

    # --- Step 2: Initial Mapping and Resource ID Assignment ---
    logger.info("Mapping raw data to actors and assigning Resource IDs...")

    # 2.1 Nodes
    initial_node_actors = [map_raw_to_node_actor(rn) for rn in raw_nodes]
    node_id_map, final_node_actors = assign_resource_ids(
        initial_node_actors, args.max_nodes_per_file, NODE_RESOURCE_PREFIX
    )
    # Create a more useful map: original_id -> NodeActor (already with resource_id)
    node_actor_map = {actor.id.split(';')[-1]: actor for actor in final_node_actors}
    logger.info(f"Resource IDs assigned to {len(final_node_actors)} nodes.")

    # 2.2 Links
    initial_link_actors_pre_dep = [] # Temporary list before resolving dependencies
    for rl in raw_links:
         link_actor_id = generate_actor_id(LINK_ACTOR_PREFIX, rl.id)
         # Create a temporary LinkActor ONLY for resource ID assignment
         temp_link_actor = type('TempLink', (), {'id': link_actor_id, 'resource_id': None})()
         initial_link_actors_pre_dep.append(temp_link_actor)

    link_id_map, _ = assign_resource_ids(
         initial_link_actors_pre_dep, args.max_links_per_file, LINK_RESOURCE_PREFIX
    )

    # Now create final LinkActors, resolving dependencies
    final_link_actors = []
    link_actor_map = {} # original_link_id -> LinkActor
    for rl in raw_links:
        original_link_id = rl.id
        link_actor_id = generate_actor_id(LINK_ACTOR_PREFIX, original_link_id)
        link_resource_id = link_id_map.get(original_link_id)
        if not link_resource_id:
             logger.error(f"Internal failure: Link {original_link_id} did not receive a resource ID.")
             continue # Or handle error differently

        link_actor = map_raw_to_link_actor(rl, global_link_attrs, node_actor_map, link_actor_id, link_resource_id)
        final_link_actors.append(link_actor)
        link_actor_map[original_link_id] = link_actor
    logger.info(f"Resource IDs assigned and dependencies resolved for {len(final_link_actors)} links.")

    # 2.3 Cars (Trips)
    initial_car_actors_pre_dep = []
    for rt in raw_trips:
        car_actor_id = generate_actor_id(CAR_ACTOR_PREFIX, rt.name) # rt.name is the car ID
        temp_car_actor = type('TempCar', (), {'id': car_actor_id, 'resource_id': None})()
        initial_car_actors_pre_dep.append(temp_car_actor)

    car_id_map, _ = assign_resource_ids(
        initial_car_actors_pre_dep, args.max_trips_per_file, CAR_RESOURCE_PREFIX
    )

    final_car_actors = []
    # car_actor_map = {} # original_trip_name -> CarActor (seems unnecessary)
    for rt in raw_trips:
        original_trip_name = rt.name
        car_actor_id = generate_actor_id(CAR_ACTOR_PREFIX, original_trip_name)
        car_resource_id = car_id_map.get(original_trip_name)
        if not car_resource_id:
             logger.error(f"Internal failure: Car/Trip {original_trip_name} did not receive a resource ID.")
             continue

        car_actor = map_raw_to_car_actor(rt, node_actor_map, link_actor_map, car_actor_id, car_resource_id)
        final_car_actors.append(car_actor)
        # car_actor_map[original_trip_name] = car_actor
    logger.info(f"Resource IDs assigned and dependencies resolved for {len(final_car_actors)} cars.")

    # --- Step 3: Split and Save ---
    logger.info("Splitting actors into files and saving...")
    node_files_info = split_and_save(final_node_actors, "nodes", scenario_output_dir, args.pretty, args.gzip)
    link_files_info = split_and_save(final_link_actors, "links", scenario_output_dir, args.pretty, args.gzip)
    car_files_info = split_and_save(final_car_actors, "cars", scenario_output_dir, args.pretty, args.gzip)

    # --- Step 4: Generate Configuration File ---
    generate_simulation_config(
        scenario_name=args.scenario_name,
        start_real_time=start_real_time_iso, # Use validated/formattted value
        duration=args.duration,
        time_unit=args.time_unit,
        time_step=args.time_step,
        start_tick=args.start_tick,
        node_files=node_files_info,
        link_files=link_files_info,
        car_files=car_files_info,
        output_dir=scenario_output_dir,
        pretty=args.pretty,
        use_gzip=args.gzip # Passed but not used for the sim config
    )

    logger.info(f"Conversion completed successfully for scenario '{args.scenario_name}'. Files at: {scenario_output_dir}")

if __name__ == "__main__":
    main()
