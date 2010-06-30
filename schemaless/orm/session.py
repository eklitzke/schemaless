class Session(object):

    def __init__(self, datastore):
        self.datastore = datastore
        self.dirty_documents = set()

    def save(self):
        for d in self.dirty_documents:
            d.save(clear_session=False)
        self.dirty_documents.clear()
