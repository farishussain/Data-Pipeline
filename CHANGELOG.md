# Changelog

All notable changes to this project will be documented in this file.  
This project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2024-11-17
### Added
- Complete data pipeline for ingesting public power, price, and installed power data from the Energy-Charts API.
- Change Data Capture (CDC) logic for incremental updates using Delta Lake.
- Data transformation scripts to support BI/ML use cases:
  - Daily price analysis for offshore and onshore wind power.
  - Daily production trend by electricity type.
  - Underperformance prediction on 30-minute intervals.
- Documentation with setup instructions, pipeline details, and next steps.
- Integration with Docker and VS Code dev containers for local development.

### Fixed
- Issue with downloading Spark binaries during container setup.

## [0.1.0] - 2024-11-10
### Added
- Initial setup of the repository with a proof of concept for:
  - Data ingestion scripts for public power and price data.
  - Basic data validation and staging area design.
  - Local development setup with Docker and Python.
- Example Delta Lake integration for local testing.

---

## Guidelines for Maintaining the Changelog
1. Use headings for each version (`## [version] - YYYY-MM-DD`).
2. Separate changes into categories:
   - **Added**: New features or functionality.
   - **Changed**: Updates to existing features.
   - **Deprecated**: Features marked for removal in future releases.
   - **Removed**: Features removed in this version.
   - **Fixed**: Bug fixes.
   - **Security**: Notable security patches.
3. List changes in chronological order, with the newest version at the top.