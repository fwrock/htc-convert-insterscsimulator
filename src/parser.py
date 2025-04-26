import logging
from typing import Tuple, List, Dict, Optional
from lxml import etree
from pathlib import Path
from .models import RawNode, RawLink, RawTrip, RawLinkAttribute, GlobalLinkAttributes

logger = logging.getLogger(__name__)

def parse_network(network_file: Path) -> Tuple[List[RawNode], List[RawLink], GlobalLinkAttributes]:
    """Reads the MATSim network XML file and extracts nodes and links."""
    logger.info(f"Starting parsing of network file: {network_file}")
    nodes: List[RawNode] = []
    links: List[RawLink] = []
    global_attrs = GlobalLinkAttributes()

    try:
        # Using iterparse for memory efficiency if needed, but getroot is simpler here
        tree = etree.parse(str(network_file))
        root = tree.getroot()

        # Extract nodes
        nodes_element = root.find('nodes')
        if nodes_element is not None:
            for node_elem in nodes_element.findall('node'):
                node_id = node_elem.get('id')
                x = node_elem.get('x')
                y = node_elem.get('y')
                if node_id and x and y:
                    nodes.append(RawNode(id=node_id, x=x, y=y))
                else:
                    logger.warning(f"Node with missing data ignored: {etree.tostring(node_elem, encoding='unicode')}")
            logger.info(f"Found {len(nodes)} nodes.")
        else:
            logger.warning("<nodes> tag not found in network file.")

        # Extract links and global attributes
        links_element = root.find('links')
        if links_element is not None:
            # Get global attributes from the <links> tag
            global_attrs.capperiod = links_element.get('capperiod')
            try:
                global_attrs.effectivecellsize = float(links_element.get('effectivecellsize', '0.0'))
            except (ValueError, TypeError):
                logger.warning("Invalid value for 'effectivecellsize', using default 0.0")
                global_attrs.effectivecellsize = 0.0
            try:
                global_attrs.effectivelanewidth = float(links_element.get('effectivelanewidth', '0.0'))
            except (ValueError, TypeError):
                logger.warning("Invalid value for 'effectivelanewidth', using default 0.0")
                global_attrs.effectivelanewidth = 0.0

            for link_elem in links_element.findall('link'):
                link_id = link_elem.get('id')
                from_node = link_elem.get('from')
                to_node = link_elem.get('to')
                length = link_elem.get('length')
                freespeed = link_elem.get('freespeed')
                capacity = link_elem.get('capacity')
                permlanes = link_elem.get('permlanes')
                oneway = link_elem.get('oneway')
                modes = link_elem.get('modes')

                if not all([link_id, from_node, to_node, length, freespeed, capacity, permlanes, oneway, modes]):
                    logger.warning(f"Link with missing attributes ignored: id={link_id}")
                    continue

                raw_link = RawLink(
                    id=link_id,
                    from_node=from_node,
                    to_node=to_node,
                    length=length,
                    freespeed=freespeed,
                    capacity=capacity,
                    permlanes=permlanes,
                    oneway=oneway,  # Might be useful for future logic, but not directly in final JSON
                    modes=modes
                )

                # Extract nested attributes
                attributes_element = link_elem.find('attributes')
                if attributes_element is not None:
                    for attr_elem in attributes_element.findall('attribute'):
                        name = attr_elem.get('name')
                        value = attr_elem.text
                        cls = attr_elem.get('class')  # Not directly used, but could be useful
                        if name and value:
                            raw_link.attributes.append(RawLinkAttribute(name=name, value=value))

                links.append(raw_link)
            logger.info(f"Found {len(links)} links.")
        else:
            logger.warning("<links> tag not found in network file.")

        return nodes, links, global_attrs

    except etree.XMLSyntaxError as e:
        logger.error(f"XML syntax error in {network_file}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while processing {network_file}: {e}")
        raise

def parse_plans(plans_file: Path) -> List[RawTrip]:
    """Reads the MATSim plans/trips XML file and extracts the trips."""
    logger.info(f"Starting parsing of plans/trips file: {plans_file}")
    trips: List[RawTrip] = []

    try:
        # Try to detect the correct format (could be <trip> or <person><plan><activity/leg>...)
        # We'll focus on the <trip> format provided in the example.
        # If using <person> format, the logic needs adjustment.

        context = etree.iterparse(str(plans_file), events=('end',), tag='trip')

        for event, elem in context:
            trip_name = elem.get('name')
            origin_node = elem.get('origin')  # In MATSim, 'origin' and 'destination' can be links or facility IDs
                                              # Assuming they are NODE IDs for the output format
            destination_node = elem.get('destination')
            link_origin = elem.get('link_origin')  # ID of the link where the vehicle starts
            count = elem.get('count', '1')  # Number of trips (not used in your output format)
            start_time = elem.get('start')
            mode = elem.get('mode')
            # digital_rails = elem.get('digital_rails_capable')  # Ignored

            # Basic validation
            if not all([trip_name, origin_node, destination_node, link_origin, start_time, mode]):
                logger.warning(f"Trip with missing attributes ignored: name={trip_name}")
                elem.clear()  # Free memory
                # The following lines are important for iterparse
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                continue

            # Only add if the mode is 'car' (or another relevant mode in the future)
            if mode and 'car' in mode.lower():  # Check if 'car' is in the mode list
                trips.append(RawTrip(
                    name=trip_name,
                    origin_node=origin_node,
                    destination_node=destination_node,
                    link_origin=link_origin,
                    count=count,
                    start_time=start_time,
                    mode=mode
                ))

            # Cleanup for iterparse
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        logger.info(f"Found {len(trips)} car trips.")
        return trips

    except etree.XMLSyntaxError as e:
        logger.error(f"XML syntax error in {plans_file}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while processing {plans_file}: {e}")
        raise
