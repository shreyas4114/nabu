import quickfix as fix
import logging

class MyApplication(fix.Application):
    def onCreate(self, session_id):
        logging.info(f"Session created: {session_id}")

    def onLogon(self, session_id):
        logging.info(f"Logon successful for session: {session_id}")

    def onLogout(self, session_id):
        logging.info(f"Logout successful for session: {session_id}")

    def toAdmin(self, message, session_id):
        logging.info(f"Outgoing admin message: {message}")

    def fromAdmin(self, message, session_id):
        logging.info(f"Incoming admin message: {message}")

def start_fix_session():
    """
    Starts the FIX session using QuickFIX.
    """
    try:
        settings = fix.SessionSettings("fix.cfg")
        app = MyApplication()
        store_factory = fix.FileStoreFactory(settings)
        log_factory = fix.FileLogFactory(settings)
        initiator = fix.SocketInitiator(app, store_factory, settings, log_factory)
        initiator.start()
        logging.info("FIX session started successfully.")
    except Exception as e:
        logging.error(f"Error starting FIX session: {e}")
