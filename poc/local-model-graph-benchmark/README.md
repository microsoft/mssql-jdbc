# Local Model Graph Benchmark

This POC compares the same local model under two conditions:

- `source`: deterministic lexical snippets from repository source and Markdown.
- `graph`: the same source retrieval plus matching nodes and edges from the generated knowledge graph.

The model, prompts, decoding settings, context budget, and repository revision remain fixed. Generated responses and timing data are written under `results/` and are intentionally ignored.

## Prerequisites

- Python 3.11 or newer
- `torch`, `transformers`, and `accelerate`
- A generated graph at `target/knowledge-graph/knowledge-graph.json` in the repository being analyzed

The source checkout and graph must represent the same revision. The graph is generated separately and is not committed because it contains machine-specific metadata and paths.

## Run

From the repository root:

```powershell
python poc/local-model-graph-benchmark/benchmark.py --case-limit 1
python poc/local-model-graph-benchmark/benchmark.py --case-id always-encrypted-key-cache
python poc/local-model-graph-benchmark/benchmark.py --prompt "How are prepared statements executed?" --condition both --verbose
python poc/local-model-graph-benchmark/benchmark.py --issue-url "https://github.com/microsoft/mssql-jdbc/issues/2957" --condition both --verbose
python poc/local-model-graph-benchmark/benchmark.py
```

Run the focused tests from the POC directory:

```powershell
cd poc/local-model-graph-benchmark
python -m unittest test_benchmark.py -v
```

`--prompt` runs one arbitrary question without using `cases.json`. `--verbose` prints the observable pipeline: model
loading, retrieval terms, source and graph evidence supplied to the model, token counts, timing, and answers. It cannot
show hidden model reasoning or chain-of-thought.

`--issue-url` reads a public GitHub issue's title and body through the GitHub API and includes them in the question sent
to the model. Combine it with `--prompt` to provide a custom analysis instruction. Private issues and API rate-limit
exhaustion require authentication, which this POC does not currently support.

The default model is `Qwen/Qwen2.5-Coder-3B-Instruct`. Its first run downloads model weights from Hugging Face. The benchmark uses deterministic greedy decoding but CPU timing can still vary between runs.

## Scoring

Score each criterion in `cases.json` as `0` (missing or wrong), `0.5` (partial), or `1` (correct). Sum each case to a score out of 5. Compare correctness, unsupported claims, citations, input/output tokens, and elapsed time. Use the same prompts and repository revision for any frontier-model baseline.

See `REPORT.md` for the completed Qwen2.5-Coder 3B experiment and tool-assisted Copilot comparison.