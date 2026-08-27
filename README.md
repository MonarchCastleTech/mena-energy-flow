# MENA Energy Flow

[![Pages](https://github.com/MonarchCastleTech/mena-energy-flow/actions/workflows/pipeline.yml/badge.svg)](https://github.com/MonarchCastleTech/mena-energy-flow/actions/workflows/pipeline.yml)

Autonomous 0–14 day MENA energy-flow disruption warning.

**Dashboard:** https://monarchcastletech.github.io/mena-energy-flow/
**Methodology:** https://monarchcastletech.github.io/mena-energy-flow/methodology/

The deterministic model combines IMF PortWatch chokepoints (35%) and energy ports (25%), FRED markets (20%), OFAC action velocity (10%), and MET Norway/ECMWF port weather (10%). GitHub Actions tests, refreshes, commits evidence, and deploys Pages every six hours. No key, account, paid API, or generative AI is required.

```bash
python -m pip install -r requirements.txt
python -m pytest -q
python pipeline/mena_energy_flow_pipeline.py
python -m http.server 8000
```

Screening signal only; not a price or conflict probability.
