import random

import pytest

from dagger.utils.utils import DAGIDGenerator


class TestDagGenerator:
    @pytest.mark.asyncio
    async def test_generator(self):
        seed1 = "order1"
        rd = random.Random()
        rd.seed(seed1)
        id1 = DAGIDGenerator.generate_dag_id_from_seed(seed=rd)
        id2 = DAGIDGenerator.generate_dag_id_from_seed(seed=rd)
        assert id1 != id2
        seed2 = "order2"
        rd.seed(seed2)
        id3 = DAGIDGenerator.generate_dag_id_from_seed(seed=rd)
        assert id3 != id1
        seed3 = "order1"
        rd = random.Random()
        rd.seed(seed3)
        id4 = DAGIDGenerator.generate_dag_id_from_seed(seed=rd)
        assert id4 == id1
