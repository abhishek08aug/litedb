# Contributing to LiteDB

Thank you for your interest in contributing! This project is an educational database built from scratch in Python. Contributions that improve clarity, correctness, or coverage of database concepts are very welcome.

---

## Ways to Contribute

- **Fix a bug** in one of the implementation files
- **Improve documentation** — clearer explanations, better diagrams, typo fixes
- **Add a new module** — e.g., buffer pool manager, bloom filter improvements, WAL checkpointing
- **Add tests** — unit tests for any of the 14 implementation modules
- **Improve demos** — better output, edge cases, performance benchmarks

---

## Getting Started

```bash
# 1. Fork the repo on GitHub, then clone your fork
git clone https://github.com/<your-username>/litedb.git
cd litedb

# 2. No dependencies to install — pure Python 3.10+ stdlib

# 3. Verify everything works before making changes
cd litedb
python run_demo.py    # should print: 7 passed, 0 failed
python run_demo.py   # should print: 13 passed, 0 failed
```

---

## Making Changes

### Branch naming

```
feature/add-buffer-pool
fix/btree-range-scan-off-by-one
docs/improve-raft-explanation
```

### Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add buffer pool manager (buffer_pool.py)
fix: correct B+ tree range scan boundary condition
docs: clarify MVCC snapshot isolation example
test: add unit tests for consistent hashing
refactor: simplify WAL replay logic
```

### Code style

- Pure Python stdlib only — no third-party packages
- Each file must be self-contained and runnable as `python <file>.py`
- Follow the existing docstring format (concept explanation at top of file)
- Keep demo output readable — use the existing `[Step N]` format

---

## Pull Request Process

1. Ensure `run_demo.py` passes (`13 passed, 0 failed`)
2. Update the relevant `README.md` if you add or change a module
3. Add an entry to `CHANGELOG.md` under `[Unreleased]`
4. Open a PR against `main` with a clear description of what and why

---

## Reporting Issues

Use the GitHub issue templates:
- **Bug report** — something produces wrong output or crashes
- **Feature request** — a concept or module you'd like to see added

---

## Code of Conduct

Be respectful and constructive. This is a learning-focused project — questions and "naive" contributions are welcome.