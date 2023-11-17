import uuid
from faker import Faker


class DataGenerator(object):

    @staticmethod
    def get_data():
        return [

                uuid.uuid4().__str__(),
                Faker().name(),
                Faker().random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
                Faker().random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
                Faker().random_int(min=10000, max=150000),
                Faker().random_int(min=18, max=60),
                Faker().random_int(min=0, max=100000),
                Faker().unix_time()
        ]
        
        
if __name__ == '__main__':
    
    print(DataGenerator.get_data())