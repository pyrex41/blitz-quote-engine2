# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Run Commands
- Start API server: `uvicorn app.main:app --reload`
- Backup database: `cp msr_target.db msr_target_copy.db`
- Check for rate changes: `python check_script.py -a -m 3 -d msr_target.db -o out.json`
- Apply rate updates: `python map_file.py -d msr_target.db -f out.json -m 3`
- Update specific carrier: `python update_carrier.py -s STATE -n NAIC -d msr_target.db -m 3`
- Rebuild mappings: `python rebuild_mapping.py -s STATE -n NAIC -d msr_target.db`

## Code Style Guidelines
- **Imports**: Standard library → third-party → local application imports
- **Type hints**: Use typing annotations for all function parameters and return values
- **Error handling**: Use try/except with detailed error messages, logging, and fallbacks
- **Naming**: snake_case for functions/variables, PascalCase for classes, UPPER_SNAKE_CASE for constants
- **Documentation**: Add docstrings to functions and classes, write descriptive variable names
- **Project structure**: FastAPI for API endpoints, SQLAlchemy for database operations
- **API design**: Use proper FastAPI routing, validation, and error handling patterns
- **Database operations**: Always create backups before modifying the database

## Safety Practices
- Check rate changes with `--dry-run` before applying updates
- Verify operations with specific state/carrier tests before running globally
- Follow the workflow in README.md for any database modifications