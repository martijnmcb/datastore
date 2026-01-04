## Coding style
- Python PEP 8, 4-space indent, ~100-char lines.
- Naming: snake_case for vars/functions, PascalCase for classes. Blueprint folder names match blueprint names; routes defined in `routes.py` within each blueprint.
- Templates: co-locate partials with consistent prefixes (e.g., `admin_*.html`).
- Keep app-wide instances in `extensions.py`; register blueprints in `app.py` with clear prefixes (e.g., `/auth`, `/beheer`).
- SQLAlchemy models live in `models.py`; config via `.env` and `config.py`.

## Docs/comments
- Use concise comments only where code is non-obvious; follow existing patterns. Keep docstrings brief; type hints not mandated but follow project norms.
