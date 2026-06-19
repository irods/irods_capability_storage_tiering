# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project **only** adheres to the following _(as defined at [Semantic Versioning](https://semver.org/spec/v2.0.0.html))_:

> Given a version number MAJOR.MINOR.PATCH, increment the:
> 
> - MAJOR version when you make incompatible API changes
> - MINOR version when you add functionality in a backward compatible manner
> - PATCH version when you make backward compatible bug fixes

## [5.1.0] - 2026-06-22

This release makes it so that the plugin can calculate checksums without triggering a permission denied error.

### Fixed

- Fix permission denied error when rodsadmin user performs checksum operation (#354).

### Added

- Add build hook option for compiling against specific version of released iRODS packages (irods/irods_development_environment#165).
