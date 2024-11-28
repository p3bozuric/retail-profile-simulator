from faker import Faker
import random
import uuid
import time

class ProfileGenerator:
    def __init__(self):

        # pip install Faker
        self.fake = Faker()

        self.categories = {
            "Mens": ["Shirts", "Pants", "Jackets and Hoodies", "Accessories"],
            "Womens": ["Dresses", "Tops", "Jackets and Hoodies", "Skirts"],
            "Kids": ["Toys", "Clothing", "School Supplies"]
        }
        self.colours = ["Red", "Blue", "Green", "Black", "White", "Purple", "Pink"]
        self.communication_methods = ["email", "push", "sms"]
        self.sizes = ["XS", "S", "M", "L", "XL", "XXL"]

    def generate_profile(self):
        """Generate a single customer profile with all required fields."""

        # Personal information
        first_name = self.fake.first_name()
        last_name = self.fake.last_name()
        gender = random.choice(['male', 'female'])
        birth_date = self.fake.date_of_birth(minimum_age=18, maximum_age=70).strftime('%Y-%m-%d')

        # Contact information
        home_address = self.fake.street_address()
        home_city = self.fake.city()
        postal_code = self.fake.postcode()
        phone_number = self.fake.phone_number()

        # Preferences
        favourite_color = random.choice(self.colours)
        favourite_category, favourite_sub_category = self._get_favourite_category()
        shirt_size, pants_size, shoe_size = self._get_sizes(gender)
        marketing_consent = random.choice([True, False])
        communication_method = random.choice(self.communication_methods)

        # System data & company related info
        join_date = self.fake.date_between(start_date='-10y', end_date='today')
        customer_id = str(uuid.uuid4().int)[:9]
        loyalty_id = int(f"2201{customer_id}")  # 2201 prefix because that's my birthday and customer ID
        loyalty_points = random.randint(0, 1000000)
        date_joined = join_date.strftime('%Y-%m-%d')
        test_profile = True
        # datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Commented out because I implemented it in SQLite

        return {
            "personal_details": {
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "date_of_birth": birth_date,
                "home_address": home_address,
                "home_city": home_city,
                "postal_code": postal_code,
                "country": "United States",  # For simplicity of randomisation // importing pycountry was also an option for this
                "iso_country_code": "US",
                "mobile_phone": phone_number,
                "email": f"{first_name.lower()}{last_name.lower()}@example.com"
            },
            "retail_preferences": {
                "favourite_color": favourite_color,
                "favourite_category": favourite_category,
                "favourite_sub_category": favourite_sub_category,
                "shirt_size": shirt_size,
                "pants_size": pants_size,
                "shoe_size": shoe_size
            },
            "marketing_preferences": {
                "consent": marketing_consent,
                "preferred_communication_method": communication_method
            },
            "loyalty_data": {
                "loyalty_number_id": loyalty_id,
                "date_joined": date_joined,
                "points": loyalty_points
            },
            "system_data": {
                "customer_id": customer_id,
                "test_profile": test_profile
            }
        }

    def generate_profiles(self, count=1, delay_range=(1, 5)):
        """Generate multiple profiles with random delays between generations."""

        delay_gen = 0

        for _ in range(count):
            time.sleep(delay_gen)
            profile_gen = self.generate_profile()
            delay_gen = random.uniform(*delay_range)
            yield profile_gen, delay_gen

    def _get_favourite_category(self):
        """For generation of random categories and subcategories from a list of dictionaries."""

        cat = random.choice(list(self.categories.keys()))
        subcat = random.choice(self.categories[cat])

        return cat, subcat

    def _get_sizes(self, gender):
        """For generation of according sizes with regard to gender"""

        shirt_size = random.choice(self.sizes)
        pants_size = random.choice(self.sizes)

        if gender=="male":
            shoe_size = random.choice(range(40, 48))
        else:
            shoe_size = random.choice(range(34, 42))

        return shirt_size, pants_size, shoe_size

# Example usage of "Profile Generator" segment of the task
if __name__ == "__main__":
    generator = ProfileGenerator()
    profiles = generator.generate_profiles(count=5, delay_range=(1, 10))

    for profile, delay in profiles:
        print(profile)
        print(delay)