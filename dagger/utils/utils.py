import random
import uuid


class DAGIDGenerator:
    @staticmethod
    def generate_dag_id_from_seed(seed: random.Random) -> uuid.UUID:
        return uuid.UUID(int=seed.getrandbits(128))
