from src.profile_generator.generator import ProfileGenerator
from src.database.db_control import DatabaseControl
from src.transform.transform import Transform
from dotenv import load_dotenv
import os
import threading
import argparse
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_database():
    """Initialize database and create necessary structures."""

    load_dotenv()

    db_params = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dbname": os.getenv("DB_NAME")
    }

    db_control = DatabaseControl(db_params)

    return db_control

def generate_and_store_profiles(db_control: DatabaseControl, generator: ProfileGenerator, num_profiles: int):
    """Generate profiles and store them in database."""
    for (profile, delay) in generator.generate_profiles(num_profiles):
        inserted_profile = db_control.insert_profile(profile)

        if inserted_profile:
            # Gets last entry from db, but isn't used for generation of json files
            # Because, why sync saving json files with this? We may be getting entries from somewhere else too.
            # We need to listen for entries.

            latest_json = db_control.get_latest_profile()
            logger.info(f"Profile generated with delay: {delay:.2f}s")
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
        db_control = setup_database()

        # Initialize transformation
        transform_json = Transform()

        # Setup for listening and beginning of listening for notifications from DB
        listener_thread = threading.Thread(target=db_control.start_listening,
                                           args=(transform_json.save_json_to_file,), # This function is the callback of start_listening - so it receives json files.
                                           daemon=True)
        listener_thread.start()

        # Setup od generator
        generator = ProfileGenerator()

        # Generate and store profiles
        generate_and_store_profiles(db_control, generator, args.num_profiles)

        logger.info("Profile generation completed successfully")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()