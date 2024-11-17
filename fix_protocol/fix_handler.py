import quickfix as fix

class MyApplication(fix.Application):
    def onCreate(self, session_id):
        print(f"Session created: {session_id}")  # You can add custom logic here if needed

    def onLogon(self, session_id):
        print("Logon successful")

    def onLogout(self, session_id):
        print("Logout successful")

    def toAdmin(self, message, session_id):
        print("Outgoing message:", message)

    def fromAdmin(self, message, session_id):
        print("Incoming message:", message)

def start_fix_session():
    settings = fix.SessionSettings("fix.cfg")
    app = MyApplication()
    store_factory = fix.FileStoreFactory(settings)
    log_factory = fix.FileLogFactory(settings)
    initiator = fix.SocketInitiator(app, store_factory, settings, log_factory)
    initiator.start()
