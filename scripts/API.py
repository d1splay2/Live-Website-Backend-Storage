import random, string, json, time, requests
from faker import Faker
from faker.providers import BaseProvider
from typing import Optional
from itertools import islice
from enum import Enum

class CustomProvider(BaseProvider):
    def user_id(self) -> int:
        return random.randint(0, 2147483647)

    def email(self) -> str:
        fake = Faker()
        return fake.ascii_free_email()

class Frequency(Enum):
    EQUAL = 1
    RANDOM = 2

class Create_Data():
    def __init__(self, schema: dict, fake: Faker):
        self.schema = schema
        self.fake = fake

    def create_normal_data(self):
        """Create random data based on column name
        by modifying original dictionary"""
        for value in self.schema:
            self.schema[value] = getattr(self.fake, value)()
        return

    def create_corrupted_data(self, random_pos: int):
        """Create random data type by modifying original dictionary"""
        for current_pos, value in enumerate(self.schema):
            if current_pos == random_pos:
                self.schema[value] = self.random_data_type()
                return
            else:
                self.schema[value] = getattr(self.fake, value)()

    def create_normal_data_from_pos(self, start: int):
        """Create random data based on column name
        from giver position by modifying orginal dictonary"""
        for value in islice(self.schema.keys(), start+1, None):
            self.schema[value] = getattr(self.fake, value)()
        return

    def random_data_type(self):
        '''Generate random data type'''
        number = random.randrange(9)
        match number:
            case 0: return ''                                                                                    # Whitespace
            case 1: return None                                                                                  # None
            case 2: return random.randrange(10000)                                                               # Int
            case 3: return random.random()                                                                       # Float
            case 4: return random.randbytes(random.randrange(20))                                                # Bytes
            case 5: return [random.random() for i in range(random.randrange(20))]                                # List
            case 6: return tuple(random.random() for i in range(random.randrange(20)))                           # Tuple
            case 7: return {random.random(): random.random() for i in range(random.randrange(20))}               # Dict
            case 8: return True if random.randint(0, 1) == 0 else False                                          # Bool
            case 9: return ''.join(random.choices(string.ascii_letters + string.digits, k=random.randrange(20))) # String

class Frequency_Generator():
    def __init__(self, amount_of_requests: int, lifetime: int):
        self.amount_of_requests = amount_of_requests
        self.lifetime = lifetime

    def equal_frequency(self) -> int:
        return self.lifetime / self.amount_of_requests

    def random_frequency(self) -> list[float]:
        weights = [random.random() for _ in range(self.amount_of_requests)]

        total_weight = sum(weights)
        intervals = [(w / total_weight) * self.lifetime for w in weights]

        return intervals

def generate_data(
                amount_of_requests: int,
                lifetime: int,
                schema: dict,
                frequency: Frequency = Frequency.RANDOM,
                randomness: Optional[float] = 0.0,
                url: str = 'http://localhost:5050/topics'
                ):
    """
    Generate random data based on schema provided using Faker library

    Args:
        amount_of_requests: How much requests should be send
        lifetime: How much seconds function should run for
        schema: Schema for json that need to be send
        frequency: How frequent request should be send in given time frame
        randomness: Chance for sending some random data type as value for schema
        url: Where data should be send
    """

    fake = Faker()
    fake.add_provider(CustomProvider)
    data = Create_Data(schema, fake)

    if frequency == Frequency.RANDOM:
        frequency = Frequency_Generator(amount_of_requests, lifetime).random_frequency()
        sleep = lambda i: time.sleep(frequency[i])
    else:
        frequency = Frequency_Generator(amount_of_requests, lifetime).equal_frequency()
        sleep = lambda i: time.sleep(frequency)

    if randomness:
        for i in range(amount_of_requests):
            if random.random() < randomness:
                random_pos = random.randrange(len(schema))

                data.create_corrupted_data(random_pos)
                data.create_normal_data_from_pos(random_pos)
            else:
                data.create_normal_data()
            sleep(i)
            requests.post(url, json=schema)
    else:
        for i in range(amount_of_requests):
            data.create_normal_data()
            sleep(i)
            requests.post(url, json=schema)

if __name__ == "__main__":
    schema = {
        'name': '',
        'country_code': '',
        'city': '',
        "street_name": '',
        'postcode': '',
        'email': '',
        'ipv4': '',
        'ipv6': '',
        'user_id': ''
    }
    generate_data(60, 1, schema)
