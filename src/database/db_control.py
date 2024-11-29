import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from typing import Dict, Any
import json
import select

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseControl:
    def __init__(self, conn_params):
        """Initialize database connection and create tables if they don't exist."""
        self.conn_params = conn_params

        # Create database if it doesn't exist
        self.create_database()

        # Create tables and set up notifications
        self.create_tables()
        self.setup_json_generation()

    def create_database(self):
        """Create database if it doesn't exist"""
        try:
            # First connect to 'postgres' database to create new database
            temp_conn = psycopg2.connect(
                dbname=self.conn_params["dbname"],
                user=self.conn_params["user"],
                password=self.conn_params["password"],
                host=self.conn_params["host"]
            )
            temp_conn.autocommit = True  # Required for creating database

            # Check if database exists
            cur = temp_conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s",
                        (self.conn_params["dbname"],))
            exists = cur.fetchone()

            if not exists:
                try:
                    cur.execute(f'CREATE DATABASE {self.conn_params["dbname"]}')
                    logger.info(f"Database {self.conn_params['dbname']} created successfully")
                except psycopg2.Error as e:
                    logger.error(f"Error creating database: {e}")
                    raise

            cur.close()
            temp_conn.close()

        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            logger.info("Please ensure PostgreSQL is running and credentials are correct")
            raise

    def get_connection(self):
        """Create and return a database connection."""
        conn = psycopg2.connect(**self.conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def create_tables(self):
        """Create necessary tables"""
        create_customers_table = """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(36) PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            gender VARCHAR(20),
            date_of_birth DATE,
            email VARCHAR(100) UNIQUE NOT NULL,
            mobile_phone VARCHAR(30),
            home_address VARCHAR(100) NOT NULL,
            city VARCHAR(50) NOT NULL,
            postal_code VARCHAR(30) NOT NULL,
            country VARCHAR(50) NOT NULL DEFAULT 'United States',
            iso_country_code CHAR(2) NOT NULL DEFAULT 'US',
            test_profile VARCHAR(6) NOT NULL,
            profile_creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

        create_customer_json_profiles_table = """
        CREATE TABLE IF NOT EXISTS customer_json_profiles (
            customer_id VARCHAR(36) PRIMARY KEY REFERENCES customers(customer_id),
            json_data JSONB,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(create_customers_table)
                cur.execute(create_retail_preferences_table)
                cur.execute(create_marketing_preferences_table)
                cur.execute(create_loyalty_members_table)
                cur.execute(create_customer_json_profiles_table)
                logger.info("Database tables created successfully")
        except psycopg2.Error as e:
            logger.error(f"Error creating database tables: {e}")
            raise

    def setup_json_generation(self):
        """Set up the JSON generation function and trigger"""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()

                # To check if trigger exists
                cur.execute("""
                                SELECT tgname FROM pg_trigger 
                                WHERE tgname = 'generate_customer_json'
                            """)
                trigger_exists = cur.fetchone()

                # Create function for JSON generation
                create_function = """
                CREATE OR REPLACE FUNCTION generate_customer_json()
                RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO customer_json_profiles (customer_id, json_data)
                    SELECT 
                        c.customer_id,
                        jsonb_build_object(
                            'createDate', c.profile_creation_date,
                            'identification', jsonb_build_object(
                                'customerId', c.customer_id,
                                'email', c.email,
                                'loyaltyId', CAST(l.loyalty_number_id AS BIGINT),
                                'phoneNumber', c.mobile_phone
                            ),
                            'individualCharacteristics', jsonb_build_object(
                                'core', jsonb_build_object(
                                    'age', CAST(EXTRACT(YEAR FROM age(c.date_of_birth::date)) AS INTEGER),
                                    'favouriteCategory', r.favourite_category,
                                    'favouriteSubCategory', r.favourite_sub_category
                                ),
                                'retail', jsonb_build_object(
                                    'favoriteColor', r.favourite_color,
                                    'pantsSize', r.pants_size,
                                    'shirtSize', r.shirt_size,
                                    'shoeSize', CAST(r.shoe_size AS INTEGER)
                                )
                            ),
                            'userAccount', jsonb_build_object(
                                'ID', c.customer_id
                            ),
                            'loyalty', jsonb_build_object(
                                'loyaltyID', CAST(l.loyalty_number_id AS BIGINT),
                                'joinDate', l.date_joined,
                                'points', CAST(l.points AS INTEGER)
                            ),
                            'consents', jsonb_build_object(
                                'collect', jsonb_build_object(
                                    'val', CASE WHEN m.marketing_consent THEN 'y' ELSE 'n' END
                                ),
                                'marketing', jsonb_build_object(
                                    'preferred', m.preferred_communication_method
                                )
                            ),
                            'homeAddress', jsonb_build_object(
                                'city', c.city,
                                'country', c.country,
                                'countryCode', c.iso_country_code,
                                'street1', c.home_address,
                                'postalCode', c.postal_code
                            ),
                            'mobilePhone', jsonb_build_object(
                                'number', c.mobile_phone
                            ),
                            'person', jsonb_build_object(
                                'birthDayAndMonth', to_char(c.date_of_birth, 'MM-DD'),
                                'birthYear', CAST(to_char(c.date_of_birth, 'YYYY') AS INTEGER),
                                'name', jsonb_build_object(
                                    'lastName', c.last_name,
                                    'fullName', c.first_name || ' ' || c.last_name,
                                    'firstName', c.first_name
                                ),
                                'gender', c.gender
                            ),
                            'personalEmail', jsonb_build_object(
                                'address', c.email
                            ),
                            'testProfile', c.test_profile
                        )
                    FROM customers c
                    JOIN retail_preferences r ON c.customer_id = r.customer_id
                    JOIN marketing_preferences m ON c.customer_id = m.customer_id
                    JOIN loyalty_members l ON c.customer_id = l.customer_id
                    WHERE l.customer_id = NEW.customer_id;

                    -- Notify about new profile
                    PERFORM pg_notify(
                        'we_got_new_amazing_client',
                        (SELECT json_data::text FROM customer_json_profiles WHERE customer_id = NEW.customer_id)
                    );

                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
                cur.execute(create_function)

                # Drop existing trigger if it exists
                cur.execute("DROP TRIGGER IF EXISTS generate_customer_json ON loyalty_members;")

                # Create trigger, but only when inserted in loyalty members because that's last input into DB
                create_trigger = """
                CREATE TRIGGER generate_customer_json
                    AFTER INSERT ON loyalty_members
                    FOR EACH ROW
                    EXECUTE FUNCTION generate_customer_json();
                """
                cur.execute(create_trigger)

                # Verify trigger creation
                cur.execute("""
                                SELECT tgname FROM pg_trigger 
                                WHERE tgname = 'generate_customer_json'
                            """)

                if cur.fetchone():
                    logger.info("Trigger created successfully!")
                else:
                    logger.error("Failed to create trigger!")

                logger.info("JSON generation system set up successfully!")

        except psycopg2.Error as e:
            logger.error(f"Error setting up JSON generation: {e}")
            raise

    def insert_profile(self, profile: Dict[str, Any]) -> bool:
        """Used for inserting data into appropriate tables."""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()

                customer_sql = """
                INSERT INTO customers (
                    customer_id, first_name, last_name, gender, date_of_birth, 
                    email, mobile_phone, home_address, city, postal_code, 
                    country, iso_country_code, test_profile
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
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
                cur.execute(customer_sql, customer_data)

                retail_sql = """
                INSERT INTO retail_preferences (
                    customer_id, favourite_color, favourite_category, 
                    favourite_sub_category, shirt_size, pants_size, shoe_size
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                retail_data = (
                    profile['system_data']['customer_id'],
                    profile['retail_preferences']['favourite_color'],
                    profile['retail_preferences']['favourite_category'],
                    profile['retail_preferences']['favourite_sub_category'],
                    profile['retail_preferences']['shirt_size'],
                    profile['retail_preferences']['pants_size'],
                    profile['retail_preferences']['shoe_size']
                )
                cur.execute(retail_sql, retail_data)

                marketing_sql = """
                INSERT INTO marketing_preferences (
                    customer_id, marketing_consent, preferred_communication_method
                ) VALUES (%s, %s, %s)
                """
                marketing_data = (
                    profile['system_data']['customer_id'],
                    profile['marketing_preferences']['consent'],
                    profile['marketing_preferences']['preferred_communication_method']
                )
                cur.execute(marketing_sql, marketing_data)

                loyalty_sql = """
                INSERT INTO loyalty_members (
                    loyalty_number_id, customer_id, date_joined, points
                ) VALUES (%s, %s, %s, %s)
                """
                loyalty_data = (
                    profile['loyalty_data']['loyalty_number_id'],
                    profile['system_data']['customer_id'],
                    profile['loyalty_data']['date_joined'],
                    profile['loyalty_data']['points']
                )
                cur.execute(loyalty_sql, loyalty_data)

                logger.info(f"Profile inserted successfully: {profile['system_data']['customer_id']}")

                return True

        except psycopg2.Error as e:
            logger.error(f"Error inserting profile: {e}")
            return False

    def start_listening(self, callback):
        """Start listening for new profile notifications with enhanced logging"""
        try:
            # Create a dedicated connection for listening that won't be garbage collected
            _conn = self.get_connection()
            _conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = _conn.cursor()

            # Listen for notifications
            cur.execute("LISTEN we_got_new_amazing_client;")
            logger.info("Started listening for new profiles...")

            while True:
                if select.select([_conn], [], [], 0.5) == ([], [], []):
                    # Keep connection alive with a simple query
                    cur.execute("SELECT 1")

                _conn.poll()
                while _conn.notifies:
                    notify = _conn.notifies.pop(0)
                    try:
                        logger.info(f"Received notification!")
                        data = json.loads(notify.payload)
                        customer_id = data.get('identification', {}).get('customerId', 'unknown')
                        logger.info(f"Processing notification for customer: {customer_id}")
                        callback(data)
                    except Exception as e:
                        logger.error(f"Error processing notification: {e}", exc_info=True)

        except psycopg2.Error as e:
            logger.error(f"Error in listener: {e}")
            raise
        finally:
            if _conn:
                _conn.close()

    def get_customer_json(self, customer_id: str) -> Dict:
        """Retrieve the JSON profile for a specific customer"""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT json_data FROM customer_json_profiles WHERE customer_id = %s",
                    (customer_id,)
                )
                result = cur.fetchone()
                return result[0] if result else None

        except psycopg2.Error as e:
            logger.error(f"Error retrieving customer JSON: {e}")
            return None

    def get_latest_profile(self) -> Dict:
        """Retrieve the most recently created customer profile"""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT json_data
                    FROM customer_json_profiles
                    ORDER BY last_updated DESC
                    LIMIT 1
                """)
                result = cur.fetchone()
                return result[0] if result else None

        except psycopg2.Error as e:
            logger.error(f"Error retrieving latest profile: {e}")
            return None