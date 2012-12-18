"""
(c) 2012 Cattura Video
(c) 2012 Eric Mangold

Somewhat simple, hopefully not broken Twisted integration for SQLAlchemy.

Hereby released under the terms of the well-known Expat license.
"""
from twisted.internet import reactor
from twisted.internet import defer
from twisted.application import service
from twisted.python import failure

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy import Column
from sqlalchemy import Integer

import threading
import Queue
import time


Base = declarative_base()


# QUEUE_WORK - this queue item represents a work item
# QUEUE_TERM this queue item represents the termination of the queue. The queue consumer
#            thread should fire the Deferred that is part of this item (tuple) on the reactor
#            thread, and then exit.
QUEUE_WORK, QUEUE_TERM = range(2)


def serializeAndThreadSession(f):
    """
    Decorator for session-using methods on the DB class.
    """
    def _(self, *args, **kw):
        if len(args) >= 1:
            if isinstance(args[0], Session):
                # We already have a session, so this means we're being called from
                # the database thread and have been explicitly called with a session object.
                # Do NOT queue this interaction, but instead execute it immediately.
                return f(self, *args, **kw)

        d = defer.Deferred()
        interaction = (QUEUE_WORK, d, f, self, args, kw)
        self.queue.put(interaction)
        return d

    return _



class DB(service.Service):
    """
    Run SQLAlchemy database interactions on a dedicated thread.
    """
    engine = None
    def __init__(self, sqlAUrl, sqlDebug=False):
        self.sqlAUrl = sqlAUrl
        self.sqlDebug = sqlDebug

        self.queue = Queue.Queue()


    def startService(self):
        service.Service.startService(self)
        
        self.thread = threading.Thread(target=self._runDB)
        #self.thread.daemon = True
        self.thread.start()


    def stopService(self):
        service.Service.stopService(self)

        if self.thread.isAlive():
            d = defer.Deferred()
            interaction = (QUEUE_TERM, d, None, None, None, None)
            self.queue.put(interaction)
            #return d # if using evil deferred-enabled services ;-)


    def _runDB(self):
        print 'Database thread started.'
        # Open database
        if not self.engine:
            self.engine = create_engine(self.sqlAUrl, echo=self.sqlDebug)
            self._Session = sessionmaker(bind=self.engine)
            Base.metadata.create_all(self.engine)

        # process work queue
        while 1:
            interaction = self.queue.get()
            flag, d, f, self, args, kw = interaction

            if flag == QUEUE_TERM:
                reactor.callFromThread(d.callback, None)
                break

            session = self._Session()
            #import pdb; pdb.set_trace()

            try:
                r = f(self, session, *args, **kw)
            except:
                session.rollback()
                reactor.callFromThread(d.errback, failure.Failure())
                continue

            session.commit()
            if isinstance(r, defer.Deferred):
                # if a Deferred result, chain it to fire in the main thread
                r.addCallbacks(lambda r: reactor.callFromThread(d.callback, r),
                               lambda f: reactor.callFromThread(d.errback, f))
            else:
                reactor.callFromThread(d.callback, r)
        print 'Database thread terminated.'



class One(Base):
    __tablename__ = 'ones'
    id = Column(Integer, primary_key=True)



class MyDB(DB):

    @serializeAndThreadSession
    def insertOne(self, session):
        one = One()
        session.add(one)
        session.flush() # needed for auto-increment to take effect.
        return one.id



if __name__ == '__main__':
    db = MyDB('sqlite://')
    db.startService()

    d = db.insertOne()

    def done(insert_id):
        print 'Done: %d' % (insert_id,)
    d.addCallback(done)

    def failed(f):
        print 'Failed: %s' % (f.value,)
    d.addErrback(failed)

    d.addBoth(lambda _: db.stopService())
    d.addBoth(lambda _: reactor.stop())

    reactor.run()
    print '__main__ exit.'
