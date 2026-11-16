# Local test data safety


Importing the application engine before configuring fixtures can retain a default database even after the environment changes. Fixtures therefore validate the actual engine before connecting, and the actual reset connection before destructive statements. Disposable sentinel regressions verify preservation on both refusal paths; SQLite URI filenames/options are rejected before path normalization.

Linux x86_64 local verification can use `.venv/bin/python -I -B scripts/run_isolated_pytest.py tests --ignore=tests/unit/test_repository_artifacts.py`. The standalone launcher establishes temporary HOME/database and Landlock ABI6 filesystem restrictions before pytest/application imports. It refuses unsupported containment and checks denial on an external disposable canary, never production writes. The supervisor verifies canary contents independently.

The native entry `.venv/bin/python -B -m pytest -p tests.isolated_bootstrap tests --ignore=tests/unit/test_repository_artifacts.py` applies the same restrictions at explicit plugin import. It requires trusted Python/pytest startup, this bootstrap first among explicit plugins, empty PYTEST_ADDOPTS, and the reviewed project configuration without earlier plugin loads. External plugins, conftest and application imports follow enforcement. Workers retain restrictions and create their own temporary storage inside inherited writable roots. Native denied canaries contain no user data and remain outside the writable tree.

These controls protect against accidental access to production storage; they do not claim a general hostile-code sandbox. TCP permissions cover ephemeral test-server ports, not destination addresses; UDP/pathname UNIX sockets and some metadata operations are outside that filter. An outer sandbox may deny TCP creation entirely, which is recorded separately from an exercised connect denial. Existing open standard streams and trusted interpreter startup remain assumptions.

Optional Bifrost executable and prepared-runtime selections add read access only. The runtime layout checks are not proof that an arbitrary directory contains only code: operators must select an independently verified code artifact, never a data/backup directory. A staged update and its backups are separate from the selected runtime.

SQLite spill files are directed to the owned temporary root once during fresh-process bootstrap, before application threads, using the startup-only temp_store_directory pragma. This is a compatibility measure: kernel denial and actual-engine checks remain the protection when a VFS ignores that advisory setting. See [SQLite temporary files](https://sqlite.org/tempfiles.html) and [startup constraints](https://sqlite.org/c3ref/temp_directory.html).

Executable lookup retains only absolute directories covered by existing source/runtime/system grants. This prevents selecting an inaccessible user-managed tool ahead of an available system executable. The filter changes only the test process environment.

Static application-source invariants scan actual source files, including untracked modules, without requiring repository metadata. Tests that exercise Git plumbing use disposable repositories containing the real migration source graph, isolated configuration and synthetic identities. They leave the working repository unchanged.

The repository-index guard in tests/unit/test_repository_artifacts.py deliberately requires the real index and remains unchanged for ordinary CI. In the local kernel test namespace, exclude that metadata-only file and separately require `git ls-files --stage -- .codex`, run from this checkout, to exit zero with empty output. This read-only repository gate imports no application or test fixtures and grants no repository metadata access to application tests.
