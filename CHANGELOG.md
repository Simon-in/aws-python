# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-01-04

### Added
- Added Salesforce integration module (`salesforce.py`)
- Added Snowflake integration module (`snowflake.py`)
- Added setup.py for project packaging
- Added pyproject.toml for modern Python project configuration
- Added MANIFEST.in for packaging configuration
- Added tests directory with initial test files
- Added CHANGELOG.md for version history
- Added CONTRIBUTING.md for contribution guidelines
- Added CODE_OF_CONDUCT.md for community behavior guidelines
- Updated requirements.txt with Salesforce and Snowflake dependencies

### Changed
- Rewrote README.md with updated module documentation
- Updated directory structure in README.md
- Replaced `bayer_cdp_common_utils` imports with `modules` imports
- Added docstrings to all module files
- Updated version to 1.1.0

### Removed
- Removed obsolete modules (dsl.py, excel.py, glue.py)

## [1.0.0] - 2026-01-02

### Added
- Initial project setup
- Core modules implementation
- AWS service integration
- S3, Redshift, DynamoDB support
- Email service functionality
- REST API interaction
- Rclone data synchronization
- Dataverse integration
- Snowflake integration
- Project documentation
