import sqlite3
import logging
from typing import Dict, Any
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseControl:
    def __init__(self, db_path: str = "retail_profiles.db"):
        """Initialize database connection and create tables if they don't exist."""

        self.db_path = db_path
        self.create_tables()

    def get_connection(self):
        """Create and return a database connection."""
        return sqlite3.connect(self.db_path)

    def create_tables(self):
        """Create necessary tables"""

        # Split into separate statements for each table
        create_customers_table = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(36) PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            gender VARCHAR(20),
            date_of_birth DATE,
            email VARCHAR(100) UNIQUE NOT NULL,
            mobile_phone VARCHAR(20),
            home_address VARCHAR(100) NOT NULL,
            city VARCHAR(50) NOT NULL,
            postal_code VARCHAR(20) NOT NULL,
            country VARCHAR(50) NOT NULL DEFAULT 'United States',
            iso_country_code CHAR(2) NOT NULL DEFAULT 'US',
            test_profile VARCHAR(6) NOT NULL,
            profile_creation_date TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%S.%f', 'now'))
        )
        """

        create_retail_preferences_table = """
        CREATE TABLE IF NOT EXISTS retail_preferences (
            customer_id VARCHAR(36) PRIMARY KEY REFERENCES customers(customer_id),
            favourite_color VARCHAR(30),
            favourite_category VARCHAR(50),
            favourite_sub_category VARCHAR(50),
            shirt_size VARCHAR(10),
            pants_size VARCHAR(10),
            shoe_size VARCHAR(10)
        )
        """

        create_marketing_preferences_table = """
        CREATE TABLE IF NOT EXISTS marketing_preferences (
            customer_id VARCHAR(36) PRIMARY KEY REFERENCES customers(customer_id),
            marketing_consent BOOLEAN DEFAULT false,
            preferred_communication_method VARCHAR(20)
        )
        """

        create_loyalty_members_table = """
        CREATE TABLE IF NOT EXISTS loyalty_members (
            loyalty_number_id VARCHAR(36) PRIMARY KEY,
            customer_id VARCHAR(36) UNIQUE REFERENCES customers(customer_id),
            date_joined DATE NOT NULL,
            points INTEGER DEFAULT 0
        )
        """

        try:
            with self.get_connection() as conn:
                # Execute each create table statement separately
                conn.execute(create_customers_table)
                conn.execute(create_retail_preferences_table)
                conn.execute(create_marketing_preferences_table)
                conn.execute(create_loyalty_members_table)
                logger.info("Database tables created successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating database tables: {e}")
            raise

    def insert_profile(self, profile: Dict[str, Any]) -> bool:

        # SQL statements for each table
        customer_sql = """
        INSERT INTO customers (
            customer_id, first_name, last_name, gender, date_of_birth, 
            email, mobile_phone, home_address, city, postal_code, 
            country, iso_country_code, test_profile
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        retail_preferences_sql = """
        INSERT INTO retail_preferences (
            customer_id, favourite_color, favourite_category, 
            favourite_sub_category, shirt_size, pants_size, shoe_size
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        marketing_preferences_sql = """
        INSERT INTO marketing_preferences (
            customer_id, marketing_consent, preferred_communication_method
        ) VALUES (?, ?, ?)
        """

        loyalty_sql = """
        INSERT INTO loyalty_members (
            loyalty_number_id, customer_id, date_joined, points
        ) VALUES (?, ?, ?, ?)
        """

        try:
            with self.get_connection() as conn:
                # Using a cursor to execute multiple commands
                cursor = conn.cursor()

                # Insert into customers table
                customer_data = (
                    profile['system_data']['customer_id'],
                    profile['personal_details']['first_name'],
                    profile['personal_details']['last_name'],
                    profile['personal_details']['gender'],
                    profile['personal_details']['date_of_birth'],
                    profile['personal_details']['email'],
                    profile['personal_details']['mobile_phone'],
                    profile['personal_details']['home_address'],
                    profile['personal_details']['home_city'],
                    profile['personal_details']['postal_code'],
                    profile['personal_details']['country'],
                    profile['personal_details']['iso_country_code'],
                    "true" if profile['system_data']['test_profile'] else "false"
                )
                cursor.execute(customer_sql, customer_data)

                # Insert into retail_preferences
                retail_data = (
                    profile['system_data']['customer_id'],
                    profile['retail_preferences']['favourite_color'],
                    profile['retail_preferences']['favourite_category'],
                    profile['retail_preferences']['favourite_sub_category'],
                    profile['retail_preferences']['shirt_size'],
                    profile['retail_preferences']['pants_size'],
                    profile['retail_preferences']['shoe_size']
                )
                cursor.execute(retail_preferences_sql, retail_data)

                # Insert into marketing_preferences
                marketing_data = (
                    profile['system_data']['customer_id'],
                    profile['marketing_preferences']['consent'],
                    profile['marketing_preferences']['preferred_communication_method']
                )
                cursor.execute(marketing_preferences_sql, marketing_data)

                # Insert into loyalty_members
                loyalty_data = (
                    profile['loyalty_data']['loyalty_number_id'],
                    profile['system_data']['customer_id'],
                    profile['loyalty_data']['date_joined'],
                    profile['loyalty_data']['points']
                )
                cursor.execute(loyalty_sql, loyalty_data)

                logger.info(f"Profile inserted successfully: {profile['system_data']['customer_id']}")
                return True

        except sqlite3.Error as e:
            logger.error(f"Error inserting profile: {e}")
            return False

    def setup_json_generation(self):
        """Set up the JSON generation trigger and related table."""

        try:
            with self.get_connection() as conn:
                # Create table for storing JSON profiles
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS customer_json_profiles (
                        customer_id VARCHAR(36) PRIMARY KEY REFERENCES customers(customer_id),
                        json_data JSON,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Drop existing trigger if it exists
                conn.execute('DROP TRIGGER IF EXISTS generate_customer_json')

                # Create new trigger
                conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS generate_customer_json
                    AFTER INSERT ON loyalty_members
                    BEGIN
                        INSERT INTO customer_json_profiles (customer_id, json_data)
                        SELECT 
                            c.customer_id,
                            json_object(
                                'createDate', c.profile_creation_date,
                                'identification', json_object(
                                    'customerId', c.customer_id,
                                    'email', c.email,
                                    'loyaltyId', l.loyalty_number_id,
                                    'phoneNumber', c.mobile_phone
                                ),
                                'individualCharacteristics', json_object(
                                    'core', json_object(
                                        'age', (strftime('%Y', 'now') - strftime('%Y', c.date_of_birth)),
                                        'favouriteCategory', r.favourite_category,
                                        'favouriteSubCategory', r.favourite_sub_category
                                    ),
                                    'retail', json_object(
                                        'favoriteColor', r.favourite_color,
                                        'pantsSize', r.pants_size,
                                        'shirtSize', r.shirt_size,
                                        'shoeSize', r.shoe_size
                                    )
                                ),
                                'userAccount', json_object(
                                    'ID', c.customer_id
                                ),
                                'loyalty', json_object(
                                    'loyaltyID', l.loyalty_number_id,
                                    'joinDate', l.date_joined,
                                    'points', l.points
                                ),
                                'consents', json_object(
                                    'collect', json_object(
                                        'val', CASE WHEN m.marketing_consent THEN 'y' ELSE 'n' END
                                    ),
                                    'marketing', json_object(
                                        'preferred', m.preferred_communication_method
                                    )
                                ),
                                'homeAddress', json_object(
                                    'city', c.city,
                                    'country', c.country,
                                    'countryCode', c.iso_country_code,
                                    'street1', c.home_address,
                                    'postalCode', c.postal_code
                                ),
                                'mobilePhone', json_object(
                                    'number', c.mobile_phone
                                ),
                                'person', json_object(
                                    'birthDayAndMonth', strftime('%m-%d', c.date_of_birth),
                                    'birthYear', strftime('%Y', c.date_of_birth),
                                    'name', json_object(
                                        'lastName', c.last_name,
                                        'fullName', c.first_name || ' ' || c.last_name,
                                        'firstName', c.first_name
                                    ),
                                    'gender', c.gender
                                ),
                                'personalEmail', json_object(
                                    'address', c.email
                                ),
                                'testProfile', c.test_profile
                            )
                        FROM customers c
                        JOIN retail_preferences r ON c.customer_id = r.customer_id
                        JOIN marketing_preferences m ON c.customer_id = m.customer_id
                        JOIN loyalty_members l ON c.customer_id = l.customer_id
                        WHERE l.customer_id = NEW.customer_id;
                    END;
                ''')

                logger.info("JSON generation system set up successfully!")
                return True

        except sqlite3.Error as e:
            logger.error(f"Error setting up JSON generation: {e}")
            return False

    def get_customer_json(self, customer_id: str) -> Dict:
        """Retrieve the JSON profile for a specific customer based on his "customer_id".
        I built this in additionally to the requested event trigger for your test."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT json_data FROM customer_json_profiles WHERE customer_id = ?",
                    (customer_id,)
                )
                result = cursor.fetchone()

                if result:
                    return json.loads(result[0])

                pass

        except sqlite3.Error as e:
            logger.error(f"Error retrieving customer JSON: {e}")
            pass

    def get_latest_profile(self) -> Dict:
        """Retrieve the most recently created customer profile JSON file."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT json_data
                    FROM customer_json_profiles
                    ORDER BY last_updated DESC
                    LIMIT 1
                """)
                result = cursor.fetchone()

                if result:
                    return json.loads(result[0])

                pass

        except sqlite3.Error as e:
            logger.error(f"Error retrieving latest profile: {e}")
            pass