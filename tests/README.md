# AsyncFlow Testing Guide

## Running Tests

- **Unit Tests Only**: `pytest -m "not integration"`
  Runs in ~1-2 seconds. Requires no external infrastructure.
  
- **Integration Tests**: `pytest -m integration`
  Runs in ~20-30 seconds. Requires the C++ queue server binary built at `queue_core/build/queue_server`. The test fixtures automatically start all necessary infrastructure (queue server, producer API, scheduler, workers) in isolated environments per test.

- **Crash Simulation**: `pytest -m crash_simulation`
  Runs in ~60-90 seconds. These tests simulate hard crashes, component restarts, and edge cases. They must be run in isolation (do not parallelize).

## Notes on Crash Simulation
- `test_lease_expiry_without_crash` is marked as `@pytest.mark.flaky` because it tests timing-sensitive lease expirations. An occasional false failure is expected if the system load causes sleep delays to drift significantly. Acceptable flakiness rate is ~5-10%. Run it repeatedly to verify it passes consistently under normal load.
