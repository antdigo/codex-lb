# Transport boundary and local verification notes

An explicit raw HTTP refusal after a proven-unsent bridge handoff must acquire a controller grant before a retry. The refusal uses the existing raw failure classification and consumes the same ordinary replay quota as transport/capacity/continuity recovery. The original deadline, publication gate and account/file restrictions survive the handoff.

A public response ID retained across opaque replay is not a portable physical anchor. When raw HTTP cannot preserve its bridge binding, the request fails locally before dispatch. For example, a public `resp_original` bound to a winning replacement response must never be sent to raw upstream as though it named that replacement. Local bindings and a durable existence-only marker recognize this case without exposing another API key's owner or response. Ordinary physical response IDs retain their raw path. Marker lookup shares the remaining request budget.

## Local tests and data integrity

Importing the application engine before configuring fixtures can retain a default database even after the environment changes. Fixtures therefore validate the actual engine before connecting, and the actual reset connection before destructive statements. Disposable sentinel regressions verify preservation on both refusal paths; SQLite URI filenames/options are rejected before path normalization.

Linux x86_64 local verification can use `.venv/bin/python -I -B scripts/run_isolated_pytest.py tests --ignore=tests/unit/test_repository_artifacts.py`. The standalone launcher establishes temporary HOME/database and Landlock ABI6 filesystem restrictions before pytest/application imports. It refuses unsupported containment and checks denial on an external disposable canary, never production writes. The supervisor verifies canary contents independently.

The native entry `.venv/bin/python -B -m pytest -p tests.isolated_bootstrap tests --ignore=tests/unit/test_repository_artifacts.py` applies the same restrictions at explicit plugin import. It requires trusted Python/pytest startup, this bootstrap first among explicit plugins, empty PYTEST_ADDOPTS, and the reviewed project configuration without earlier plugin loads. External plugins, conftest and application imports follow enforcement. Workers retain restrictions and create their own temporary storage inside inherited writable roots. Native denied canaries contain no user data and remain outside the writable tree.

These controls protect against accidental access to production storage; they do not claim a general hostile-code sandbox. TCP permissions cover ephemeral test-server ports, not destination addresses; UDP/pathname UNIX sockets and some metadata operations are outside that filter. An outer sandbox may deny TCP creation entirely, which is recorded separately from an exercised connect denial. Existing open standard streams and trusted interpreter startup remain assumptions.

Optional Bifrost executable and prepared-runtime selections add read access only. The runtime layout checks are not proof that an arbitrary directory contains only code: operators must select an independently verified code artifact, never a data/backup directory. A staged update and its backups are separate from the selected runtime.
