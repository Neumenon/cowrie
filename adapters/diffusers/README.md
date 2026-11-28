# Cowrie + Hugging Face Diffusers (`CowrieDiffusersCache`)

Cache expensive text-encoder passes (`UMT5-XXL`, `T5-XXL`, `Qwen3`, `CLIP`) across restarts, seed iterations, and CFG sweeps in 16 KB-aligned, `0600` HMAC/SHA-NI-verified `.state.cowrie` (`CWSP`) shards keyed by canonical `COWR` CIDs (immune to prompt-dict key ordering).

## 3-Line Drop-In

```python
import torch
from diffusers import FluxPipeline
from cowrie.integrations.diffusers import wrap_pipeline_encode_prompt

pipe = FluxPipeline.from_pretrained("black-forest-labs/FLUX.1-dev", torch_dtype=torch.bfloat16).to("cuda")
cache = wrap_pipeline_encode_prompt(pipe, cache_dir="~/.cache/cowrie/diffusers")

# First run encodes prompt + writes 16 KB CWSP shard; subsequent restarts/seed tweaks hit disk in milliseconds.
image = pipe("editorial studio portrait, 85mm lens", generator=torch.Generator("cuda").manual_seed(42)).images[0]
print(f"hits={cache.hits} misses={cache.misses}")
```

Set `COWRIE_HMAC_KEY=<64-hex>` to authenticate every cached shard with RFC 2104 `HMAC-SHA256` before `mmap` loading.
