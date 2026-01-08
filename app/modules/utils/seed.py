import random
import os
import logging
import numpy as np
from app.modules.settings.settings import DEFAULT_SEED

logger = logging.getLogger(__name__)

class SeedUtil:
    instance = None

    # __new()__をオーバーライド
    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(cls)

        return cls.instance

    def __init__(self, n_seed: int):
        if not hasattr(self, 'seed'):
            self.n_seed = n_seed

    @classmethod
    def get_instance(cls, n_seed : int = DEFAULT_SEED) -> "AppSettings":
        if cls.instance is None:
            if n_seed is None:
                n_seed = DEFAULT_SEED

            cls.instance = cls(n_seed)

        return cls.instance

    @classmethod
    def reset_instance(cls):
        cls.instance = None

    def get_seed(self) -> int:
        return self.n_seed

    def seed_everything(self) -> None:
        random.seed(self.n_seed)
        os.environ['PYTHONHASHSEED'] = str(self.n_seed)
        np.random.seed(self.n_seed)
