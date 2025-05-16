# MATSim to Hyperbolic Time Chamber (HTC) Converter

This script converts network (`network.xml`) and plans/trips (`plans.xml`, `trips.xml`, etc.) files from the MATSim format into the JSON format expected by the InterSCITY HTC-Simulator.

It splits the data of nodes, links, and cars (trips) into multiple JSON files based on configurable limits and generates a main `simulation.json` file that references all the generated data files.

## Structure

- `convert_matsim.py`: Main script for command-line execution.
- `src/`: Contains the modularized source code.
  - `parser.py`: Responsible for reading and parsing the input XML files.
  - `models.py`: Defines the data structures (dataclasses) for intermediate and final objects.
  - `splitter.py`: Maps the raw data into the final format, assigns `resourceId`s, resolves dependencies, and splits/saves the JSON files.
  - `simulation_gen.py`: Generates the `simulation.json` file.
  - `utils.py`: Utility functions (logging, saving files, etc.).
- `output/`: Default directory where the converted scenarios will be saved (e.g., `output/my_scenario/`).
- `requirements.txt`: Python dependencies.
- `tests/`: Directory for future unit tests.

## Prerequisites

- Python 3.10+
- Libraries listed in `requirements.txt`

## Installation

1. Clone the repository (or copy the files).
2. Create a virtual environment
    ```bash
    python3 -m venv htc-coverter-env
    ```
3. Activate the virtual environment
    ```bash
    source htc-coverter-env/bin/activate   # On Linux/macOS
    ```
    ```bash
     .\htc-coverter-env\Scripts\activate   # On Windows
    ```
4. Install the dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

Run the main script `convert_matsim.py` from the root directory (`htc-converter-matsim/`) providing the paths to the input files and other desired options.

```bash
python convert_matsim.py --network /path/to/your/network.xml --plans /path/to/your/plans.xml [OPTIONS]
