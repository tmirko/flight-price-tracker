# Project agent guidelines

## Docstrings (required)

- Every **Python module**, **class**, and **callable** must have a docstring, including:
  - Public APIs and private helpers (leading `_`)
  - Test modules and test functions
- Follow the Google Python Style Guide docstring conventions:
  https://google.github.io/styleguide/pyguide.html#s3.8-comments-and-docstrings

### Style requirements

- Use an **imperative** one-line summary ending with a period.
- Add a longer description if it adds clarity.
- When applicable, include sections in this order:
  - `Args:` (one entry per parameter, with a description)
  - `Returns:`
  - `Raises:`
- Keep types in the type hints; do not duplicate them in docstrings unless necessary for clarity.

### Enforcement

- `ruff` must enforce docstrings (pydocstyle `D*` rules) with Google convention.
- Do not add per-file ignores to skip docstring requirements.
