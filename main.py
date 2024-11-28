import argparse
import logging
from src.profile_generator.generator import ProfileGenerator
from src.database.db_control import DatabaseControl
import json
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_database():
    """Initialize database and create necessary structures."""
    db = DatabaseControl()
    # Create tables and trigger
    db.setup_json_generation()
    return db

def save_json_to_file(profile_data: dict):
    """
    Saves a profile JSON to a file in "results" directory.

    Args:
        profile_data: Dictionary containing the profile data
    """

    output_dir = 'results'

    # Generate filename using customer ID and timestamp
    customer_id = profile_data['identification']['customerId']
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"profile_{customer_id}_{timestamp}.json"

    # Create full file path
    file_path = f"{output_dir}/{filename}"

    # Save JSON with proper formatting
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(profile_data, f, indent=2, ensure_ascii=False)

    return file_path

def generate_and_store_profiles(db: DatabaseControl, generator: ProfileGenerator, num_profiles: int):
    """Generate profiles and store them in database."""
    for (profile, delay) in generator.generate_profiles(num_profiles):
        inserted_profile = db.insert_profile(profile)

        if inserted_profile:
            # Get and log the JSON version of the profile
            latest_json = db.get_latest_profile()
            filepath = save_json_to_file(latest_json)
            logger.info(f"Profile generated and stored to: '{filepath}' with delay: {delay:.2f}s")
        else:
            logger.error("Failed to insert profile")

def main():

    # Set up argument parser so this script can be accessed using command line.
    parser = argparse.ArgumentParser(description='Generate and store retail customer profiles')
    parser.add_argument(
        '--num-profiles',
        type=int,
        default=10,
        help='Number of profiles to generate (default: 10)'
    )
    args = parser.parse_args()

    try:
        logger.info("Starting profile generation process...")

        # Initialize components
        db = setup_database()
        generator = ProfileGenerator()

        # Generate and store profiles
        generate_and_store_profiles(db, generator, args.num_profiles)

        logger.info("Profile generation completed successfully")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()