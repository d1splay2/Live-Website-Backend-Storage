from faker import Faker
from enum import Enum
import random
import json
import time
import requests


class Frequency(Enum):
    EQUAL = 1
    RANDOM = 2

def main(amount_of_requests: int, lifetime: int, frequency: Frequency):
    url = 'http://localhost:5000'
    faker = Faker()
    person = {
        'name': '',
        'address': ''
    }

    match frequency:
        case Frequency.EQUAL: frequency = equal_frequency(amount_of_requests, lifetime)
        case Frequency.RANDOM: frequency = random_frequency(amount_of_requests, lifetime)

    for i in range(amount_of_requests):
        time.sleep(frequency[i])
        person['name'], person['address'] = faker.name(), faker.address()
        requests.post(url, json=person)
        

def equal_frequency(amount_of_requests: int, lifetime: int):
    return lifetime / amount_of_requests

def random_frequency(amount_of_requests: int, lifetime: int):
    weights = [random.random() for _ in range(amount_of_requests)]
    
    total_weight = sum(weights)
    intervals = [(w / total_weight) * lifetime for w in weights]

    return intervals

main(5, 20, Frequency.RANDOM)