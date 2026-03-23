import pytest


@pytest.fixture(autouse=True)
def shutdown_queue_manager(request):
    """
    Autouse fixture: after each test, gracefully stop any QueueManager
    that was created via getMockAppInstance().

    Without this, the DatabaseWorker thread (daemon=True after the source
    fix) would still hold open DB connections unnecessarily between tests.
    With daemon=True on the thread this fixture is belt-and-suspenders, but
    it also makes test isolation cleaner and avoids semaphore leaks.
    """
    yield   # test runs here

    # Teardown: find the module-level app_inst globals across all test modules
    # and shut them down cleanly.
    for module_name in list(request.session._collected_tests_by_module
                            if hasattr(request.session, '_collected_tests_by_module')
                            else []):
        pass  # only needed for reference

    # Direct approach: inspect the calling test module's globals
    module = request.module
    app_inst = getattr(module, 'app_inst', None)
    if app_inst is not None:
        qm = getattr(app_inst, 'queue_manager', None)
        if qm is not None:
            shutdown_event = getattr(qm, 'shutdown_event', None)
            if shutdown_event is not None and not shutdown_event.is_set():
                try:
                    shutdown_event.set()
                    # Drain the DB command queue with a poison pill
                    db_q = getattr(qm, 'dbCommandQueue', None)
                    if db_q is not None:
                        db_q.put(None)
                except Exception:
                    pass
