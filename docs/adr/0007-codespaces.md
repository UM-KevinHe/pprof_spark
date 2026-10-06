# ADR-0007: Codespaces development environment (spike S-06)

- Status: Accepted (delegated; D-19); the maintainer's trial is pending
- Date: 2026-10-06

## Context
The maintainer cannot install Scala or Spark locally (§11.1). CI runs everything; a codespace would
add interactive sbt, local Spark tests and `git apply` in the browser, if institutional policy
allows GitHub Codespaces. Protected data never enter a codespace (NN-7).

## Decision
`.devcontainer/` uses `mcr.microsoft.com/devcontainers/java:21-bookworm` (JDK 21), installs the sbt
version pinned in `project/build.properties` from sbt's GitHub release, downloads the build's
dependencies once, and adds the Metals extension; it asks for 4 cores and 8 GB. CI stays the
source of evidence; a codespace is a convenience.

## Evidence
Configuration only: the JSON parses, the setup script passes `bash -n`, and the image tag is
listed by the devcontainers/images repository. The spike passes when the maintainer creates a
codespace (Code, then Codespaces, then Create) and `sbt ci` succeeds in it.

## Consequences
If Codespaces is not allowed, the working alternative stays as it is: patches applied with git,
CI as the build and test environment, and the assistant's sandbox emulation before each patch.
