# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project **only** adheres to the following _(as defined at [Semantic Versioning](https://semver.org/spec/v2.0.0.html))_:

> Given a version number MAJOR.MINOR.PATCH, increment the:
> 
> - MAJOR version when you make incompatible API changes
> - MINOR version when you add functionality in a backward compatible manner
> - PATCH version when you make backward compatible bug fixes

## [4.3.3.1] - 2025-02-07

This release includes coverage of more PEPs and resolves all issues stemming from a lack of user permissions.

The Storage Tiering capability plugin now uses the local rodsadmin account to carry out all operations related to data movement and metadata manipulation.

The iRODS Consortium recommends updating custom violating queries to use lowercase **select** to avoid issues with GenQuery1. See issue #281 for details.

### Changed

- Optimize retrieval of configuration values (#178, #297).
- Execution of storage tiering rules requires rodsadmin level privileges (#293).
- Replace `proxy_connection` with `client_connection` (#296).
- Restage data objects accessed via `rcDataObjOpen` (#303).

### Fixed

- Run plugin operations as rodsadmin (#164, #175, #189, #203, #273).
- Use lowercase **select** to find data objects in violation (#281).
- Fix memory leaks (#288).
- Use correct cleanup function in test suite for removal of object (#308).
- Enable use of 4.3 logger (#331).
- Fix EL8/9 package installation error (#336).

### Added

- Update access time metadata on touch API PEPs (#266).
- Add `--debug_build` option to build hook (#286).
- Add support for compiling with Address Sanitizer (#287).
- Update access time metadata on get API PEPs (#312).
- Update access time metadata on replica_open/close API PEPs (#316).
