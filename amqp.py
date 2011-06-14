"""
"""

import os
import sys
from time import time

from zope.interface import implements

from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet import protocol
from twisted.python import failure
from twisted.python import usage
from twisted.python import log
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from txamqp import spec
from txamqp.content import Content
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient, AMQChannel, Frame
from txamqp.queue import TimeoutDeferredQueue

SPEC_PATH = os.path.join(os.path.abspath('.'), 'amqp0-8.xml')

class HeartbeatExpired(Exception):
    """
    """

class ChannelWithCallback(AMQChannel):

    def __init__(self, *args):
        AMQChannel.__init__(self, *args)
        self.deliver_callback = lambda dev_null: dev_null
    
    def set_consumer_callback(self, callback):
        self.deliver_callback = callback

class AMQPProtocol(AMQClient):
    """ Improvements to the txamqp implementation.
    """

    channelClass=ChannelWithCallback
    next_channel_id = 0
    closed = True

    def channel(self, id=None):
        """Overrides AMQClient. Changes: 
            1) no need to return deferred. The channelLock doesn't protect
            against any race conditions; the channel reference is returned,
            so any number of those references could exist already. 
            2) auto channel numbering
            3) replace deferred queue for basic_deliver(s) with simple
               buffer(list)
        """
        if id is None:
            self.next_channel_id += 1
            id = self.next_channel_id
        try:
            ch = self.channels[id]
        except KeyError:
            ch = self.channelFactory(id, self.outgoing)
            self.channels[id] = ch
        return ch

    def queue(self, key):
        """ This is the channel basic_deliver queue
        """
        try:
            q = self.queues[key]
        except KeyError:
            q = TimeoutDeferredQueue()
            self.queues[key] = q
        return q

    def connectionMade(self):
        self.closed = False
        AMQClient.connectionMade(self)

    def processFrame(self, frame):
        ch = self.channel(frame.channel)
        if frame.payload.type == Frame.HEARTBEAT:
            self.lastHBReceived = time()
            log.msg('Heartbeat received %s' % (str(self.lastHBReceived),))
        else:
            ch.dispatch(frame, self.work)
        if self.heartbeatInterval > 0:
            self.reschedule_checkHB()

    def frameLengthExceeded(self):
        """
        """

    def checkHeartbeat(self):
        if self.checkHB.active():
            self.checkHB.cancel()
        log.msg('AMQP Heartbeat expired on client side')
        self.transport.loseConnection(failure.Failure(HeartbeatExpired('Heartbeat expired.')))

    def reschedule_sendHB(self):
        log.msg('Reschedule send heartbeat')
        AMQClient.reschedule_sendHB(self)


    def reschedule_checkHB(self):
        log.msg('Reschedule check heartbeat')
        AMQClient.reschedule_checkHB(self)

 
class ConnectionCreator(object):
    """Create AMQP Client.
    The AMQP Client uses one persistent connection, so a Factory is not
    necessary.

    Client Creator is initialized with AMQP Broker configuration.

    ConnectTCP is called with TCP configuration.
    """

    protocol = AMQPProtocol
    spec_cache = {}

    def __init__(self, reactor, username='guest', password='guest',
                                vhost='/', delegate=None,
                                spec_path=SPEC_PATH,
                                heartbeat=0):
        self.reactor = reactor
        self.username = username
        self.password = password
        self.vhost = vhost

        # Cache the specs for enormous speedups on subsequent connections
        cache = ConnectionCreator.spec_cache
        if spec_path in cache:
            self.spec = cache[spec_path]
        else:
            self.spec = cache.setdefault(spec_path, spec.load(spec_path))

        self.heartbeat = heartbeat
        if delegate is None:
            delegate = TwistedDelegate()
        self.delegate = delegate
        self.connector = None

    def connectTCP(self, host, port, timeout=30, bindAddress=None):
        """Connect to remote Broker host, return a Deferred of resulting protocol
        instance.
        """
        d = defer.Deferred()
        p = self.protocol(self.delegate,
                                    self.vhost,
                                    self.spec,
                                    heartbeat=self.heartbeat)
        p.factory = self
        f = protocol._InstanceFactory(self.reactor, p, d)
        self.connector = self.reactor.connectTCP(host, port, f, timeout=timeout,
                bindAddress=bindAddress)
        def auth_cb(conn):
            d = conn.authenticate(self.username, self.password)
            d.addCallback(lambda _: conn)
            return d
        d.addCallback(auth_cb)
        return d


class AMQPEvents(TwistedDelegate):
    """
    This class defines handlers for asynchronous amqp events (events the
    broker can raise at any time).
    """

    def basic_deliver(self, ch, msg):
        """
        """
        #d = defer.maybeDeferred(ch.deliver_callback, msg)

    def basic_return(self, ch, msg):
        """implement this to handle messages that could not be delivered to
        a queue or consumer
        """
        log.msg("""basic.return event received! This means the broker\
                could not guarantee reliable delivery for a message sent\
                from a process in this container.""")

    def channel_flow(self, ch, msg):
        """implement this to handle a broker flow control request
        """
        log.msg('channel.flow event received')

    def channel_alert(self, ch, msg):
        """implement this to handle a broker channel.alert notification.
        """
        log.msg('channel.alert event received')

    def close(self, reason):
        """The AMQClient protocol calls this as a result of a
        connectionLost event. The TwistedDelegate.close method finishes
        shutting off the client, and we get a chance to react to the event
        here.
        """
        TwistedDelegate.close(self, reason)
        log.err("AMQP Client Connection Closed")
        log.err(reason)

class Options(usage.Options):

    optParameters = [
            ['broker_host', 'h', 'localhost', ''],
            ['heartbeat', 'b', 0, 'Heartbeat in seconds'],
            ]

def main():

    log.startLogging(sys.stdout)

    config = Options()
    config.parseOptions()
    print type(config['heartbeat'])

    amqpEvents = AMQPEvents()
    clientCreator = ConnectionCreator(reactor,
                                vhost='/',
                                delegate=amqpEvents,
                                heartbeat=int(config['heartbeat']),
                                username='guest',
                                password='guest')
    d = clientCreator.connectTCP(config['broker_host'], 5672)

    def connected(client):
        log.msg('AMQP Client Connected')
        return client
    d.addCallback(connected)
    d.addErrback(defer.logError)

if __name__ == "__main__":
    main()
    reactor.run()
