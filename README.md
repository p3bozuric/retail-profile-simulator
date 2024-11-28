# Profile Simulator for Retail Industry

This is my version of a profile simulator for retail industry. It was built for IBM iX Data Engineering entry test case
This simulates the creation of customer profiles and their ingestion into a marketing system. The application generates random customer profiles, stores them in a SQLite database, and automatically transforms them into JSON format.

## Features
- Random customer profile generation using Faker
- SQLite database storage
- Automatic JSON transformation
- Configurable number of profiles to generate
- Random delays between profile generations

## Setup

1. Clone the repository
```bash
git clone https://github.com/p3bozuric/retail-profile-simulator
cd retail-profile-simulator
```

2. Create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

## Usage

Generate profiles with default settings (10 profiles):
```bash
python main.py
```

Generate specific number of profiles:
```bash
python main.py --num-profiles 5
```

## Project Structure
```
retail-profile-simulator/
├── main.py                 # Application entry point
├── requirements.txt        # Dependencies
├── results/                # Generated JSON profiles (currently inside from my test run)
└── src/                    # Source code
    ├── database/           # Database operations
    │   ├── __init__.py
    │   └── db_control.py
    ├── profile_generator/  # Profile generation
    │   ├── __init__.py
    └── └── generator.py
```

## Implementation Details

### Database Schema
- customers: Base customer information
- retail_preferences: Customer's retail preferences
- marketing_preferences: Marketing consent and preferences
- loyalty_members: Loyalty program information

### Generated Profile Structure
Profiles are generated with:
- Personal details (name, address, contact info)
- Retail preferences (sizes, favorite categories)
- Marketing preferences
- Loyalty data
- System data

### Output
JSON profiles are automatically saved to the 'results' directory with format:
```
profile_[customer_id]_[timestamp].json
```

## Dependencies
- Python 3.8+
- Faker (for generating realistic data)
- SQLite3 (included in Python)

## Documentation
For more detailed information about specific components, check the docstrings & comments in the source code.