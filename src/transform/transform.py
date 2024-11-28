from datetime import datetime
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Transform:

    def __init__(self, output_dir: str = "results"):
        """Object for saving json. Call save_json_to_file after this to save.

        Args:
            output_dir: Dictionary containing the profile data"""

        self.output_dir = output_dir

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")

    def save_json_to_file(self, profile_data: dict):
        """
        Save profile data to JSON file.

        Args:
            profile_data: Dictionary containing the profile data
        """
        try:
            # Extract customer ID from the nested structure
            customer_id = profile_data.get('identification', {}).get('customerId', 'unknown')

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            filename = f"profile_{customer_id}_{timestamp}.json"

            # Create full file path
            file_path = os.path.join(self.output_dir, filename)

            # Save JSON with proper formatting
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(profile_data, f, indent=2, ensure_ascii=False)

            logger.info(f"Profile saved to: {file_path}")

        except Exception as e:
            logger.error(f"Error saving profile to file: {e}")
            raise