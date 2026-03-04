from faker import Faker
from faker.providers import BaseProvider
from enum import Enum
import random
import string
import json
import time
import requests

class CustomProvider(BaseProvider):
    def user_id(self) -> int:
        return random.randint(0, 2147483647)

    def email(self) -> str:
        fake = Faker()
        return fake.ascii_free_email()


class Frequency(Enum):
    EQUAL = 1
    RANDOM = 2

def main(amount_of_requests: int, lifetime: int, frequency: Frequency, randomness: float = 0.0):
    person = {
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

    match frequency:
        case Frequency.EQUAL:
            frequency = equal_frequency(amount_of_requests, lifetime)
            for _ in range(amount_of_requests):
                time.sleep(frequency)
                generate_person(person, randomness)
                # data = generate_person(person, randomness)
                # requests.post(url, json=data)
        case Frequency.RANDOM:
            frequency = random_frequency(amount_of_requests, lifetime)
            for i in range(amount_of_requests):
                time.sleep(frequency[i])
                generate_person(person, randomness)
                # data = generate_person(person, randomness)
                # requests.post(url, json=data)

def generate_person(struct: dict, randomness: float):
    url = 'http://localhost:5000'
    fake = Faker()
    fake.add_provider(CustomProvider)
    if random.random() < randomness:
        random_pos = random.randrange(len(struct))
        temp = 0
        print(random_pos)
        for i in struct:
            if random_pos == temp:
                struct[i] = random_data_type()
                temp += 100
            else:
                struct[i] = getattr(fake, i)()
                temp += 1
        requests.post(url, json=struct)
        # return struct
    else:
        for i in struct:
            struct[i] = getattr(fake, i)()
        requests.post(url, json=struct)
        # return struct

def equal_frequency(amount_of_requests: int, lifetime: int) -> int:
    return lifetime / amount_of_requests

def random_frequency(amount_of_requests: int, lifetime: int) -> list[float]:
    weights = [random.random() for _ in range(amount_of_requests)]

    total_weight = sum(weights)
    intervals = [(w / total_weight) * lifetime for w in weights]

    return intervals

def random_data_type():
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


main(10, 1, Frequency.EQUAL)
# print(random_data_type())
