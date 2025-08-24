# Profile Simulator for Retail Industry

This is my version of a profile simulator for retail industry.
This simulates the creation of customer profiles and their ingestion into a marketing system. The application generates random customer profiles, stores them in a PostgreSQL database, and automatically transforms them into JSON format.

## Features
- Random customer profile generation using Faker
- PostgreSQL database storage with automatic JSON transformation
- Real-time notifications for new profiles
- Configurable number of profiles to generate
- Random delays between profile generations

## Prerequisites

1. PostgreSQL server installed and running
2. Create a `.env` file in the root directory with the following variables:
```bash
DB_NAME=your_database_name
DB_USER=your_postgres_user
DB_PASSWORD=your_postgres_password
DB_HOST=your_host_address
```

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
├── .env                    # YOU NEED TO CREATE THIS FILE
├── requirements.txt        # Dependencies
├── results/                # Generated JSON profiles - will be created when you run main.py
└── src/                    # Source code
    ├── database/           # Database operations
    │   ├── __init__.py
    │   └── db_control.py
    ├── profile_generator/  # Profile generation
    │   ├── __init__.py
    │   └── generator.py
    ├── transform/          # Transformation of data
    │   ├── __init__.py
    └── └── transform.py
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
- psycopg2-binary (PostgreSQL adapter for Python)
- python-dotenv (for environment variables)

## Documentation
For more detailed information about specific components, check the docstrings & comments in the source code.
