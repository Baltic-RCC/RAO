from dataclasses import dataclass
import pandas as pd

@dataclass
class CracWorkaroundContext:
    enable_3w_trafo_replacement: bool = False
    replaced_3w_trafos: pd.DataFrame | None = None

    def has_3w_replacement(self) -> bool:
        return(
            self.enable_3w_trafo_replacement
            and self.replaced_3w_trafos is not None
            and not self.replaced_3w_trafos.empty
        )

