'''
Created on Sep 28, 2015

@author: jackie
'''

import multiprocessing.dummy as multiprocessing
import copy
import socket
import logging
import sys
from random import randint
sys.path.append("../modules")
from Common import nameServiceLocation
from Common.orb import Request
from Common.orb import Stub
from Common.orb import ProtocolError
from Common.readWriteLock import ReadWriteLock

# -----------------------------------------------------------------------------
# Initialize and read the command line arguments
# -----------------------------------------------------------------------------

description = """\
Name server for a group of peers. It allows peers to find each other by object_id.\
"""

server_address = nameServiceLocation.name_service_address

# -----------------------------------------------------------------------------
# The Name Server
# -----------------------------------------------------------------------------

logging.basicConfig(format="%(levelname)s:%(filename)s: %(message)s", level=logging.INFO)

class NameServer(object):
    """Class that handles peers."""
    
    def __init__(self):
        self.lock = ReadWriteLock()
        self.next_id = 0
        self.responses = dict()
        self.peers = dict()

    # The dictionary self.peers assigns a dict to each object type
    # This dict maps the id of each peer to their address (id: addr)

    # So for example, self.peers could look like this:

    # obj_type (key)    | peers (value)
    # ------------------+------------------------------------------------------
    # [group0]          | {0: (addr0, port0), 3: (addr3, port3)}
    # [group1]          | {1: (addr1, port1), 4: (addr4, port4), 5: (addr5, port5)}
    # [group2]          | {2: (addr2, port2)}

    def register(self, obj_type, fullAddress):
        logging.debug("NameServer registering peer at {}".format(fullAddress))

        # Make sure everyone in our group is still alive
        self._check_all_alive(obj_type) 

        # The address might come in another format
        fullAddress = tuple(fullAddress)
        
        # Set the hash to the address (for now)
        obj_hash = fullAddress

        # We're locking since we're making changes to self.next_id
        self.lock.write_acquire()
        obj_id = self.next_id
        self.next_id += 1
        self.lock.write_release()

        # We can't have anything locked when we call this
        group = self._get_group(obj_type)

        # Add the address to our group of servers
        self.lock.write_acquire()
        group[obj_id] = obj_hash
        self.lock.write_release()
        
        logging.info("NameServer done registering peer at {}".format(fullAddress))
        return (obj_id, obj_hash)

    def unregister(self, obj_id, obj_type, obj_hash):
        logging.debug("NameServer unregistering peer at {}".format(tuple(obj_hash)))

        # The hash might come in another format
        obj_hash = tuple(obj_hash)
        
        group = self._get_group(obj_type)
        # Ensure what they're trying to remove matches our data exactly
        if obj_id in group and (group[obj_id] == obj_hash):
            self._remove_peer(obj_type, obj_id)

        # If not, it's probably our fault
        else:
            logging.debug("\nERR: Unregistering peer not registered!\n{}"
                          .format((obj_id,obj_type,obj_hash)))
        
	# This function doesn't stop until every peer has been checked.
        # BUT there's a peer out there waiting for this function to return
        # Before it can be unregistered.
        # This is very obnoxious.
        logging.info("NameServer done unregistering peer at {}".format(tuple(obj_hash)))
        self._check_all_alive(obj_type) # Make sure everyone in our group is still alive
        return "null"
    
    def get_peers(self, obj_type):
        """ Returns a copy of our dictionary of peers for any obj_type. """
        peers = self._get_group(obj_type)

        # We want to lock this in case someone tries to change it
        self.lock.read_acquire()
        peers = copy.deepcopy(peers)
        self.lock.read_release()
        
        return peers

    def require_any(self, server_type):
        """ What is this supposed to return, just the address? """        
        thosePeers = self.get_peers(server_type)
        
        # Pick a random peer
        randomKey = thosePeers.keys()[randint(0,len(thosePeers)-1)]
        randompeer = thosePeers[randomKey]
        
        return randomPeer[1] # This should be the (address,port)

    def require_object(self, server_type, server_id):
        thosePeers = self.get_peers(server_type)

        # Don't return something that doesn't exist, of course
        if server_id in thosePeers:
            return thosePeers[server_id]
        else:
            return None

    # -------------------------------------------------------------------------
    # Private Methods
    # -------------------------------------------------------------------------

    def _remove_peer(self, obj_type, obj_ID):
        group = self._get_group(obj_type)
        logging.info("Removing peer {}.".format(obj_ID))
        self.lock.write_acquire()
        del group[obj_ID]
        self.lock.write_release()
    
    def _get_group(self, obj_type):
        """
        Returns a dictionary of our obj_type, complete with
        locking and creating the dictionary if it doesn't already exist.
        """
        # Create our group if it doesn't already exist
        if obj_type not in self.peers:
            self.lock.write_acquire()
            self.peers[obj_type] = dict()
            self.lock.write_release()
        
        # Fetch and return the group
        self.lock.read_acquire()
        group = self.peers[obj_type]
        self.lock.read_release()

        # Make sure to lock.read_acquire() when using this group,
        #       and to lock.read_release() when you're done!
        return group

    # These aren't imported from the orb because the nameServer doesn't store
    # things inside peerLists, but instead inside dicts within dicts.
    
    def _check_all_alive(self, obj_type):
        logging.info("NameServer confirming connections to all peers" \
                 + " of type {}.".format(obj_type))
        peers = self.get_peers(obj_type)
        for peer in peers.values():
            self._check_alive(obj_type, peer)
    
    def _check_alive(self, obj_type, peer):
        logging.debug("NameServer confirming connection to peer {}.".format(peer[0]))

        # If it's dead, clean it off the list.
        if not self._is_alive(obj_type, peer, 5):
            self._remove_peer(obj_type, peer)
    
    def _is_alive(self, obj_type, peer, timeout=5):
        try:
            parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
            p = multiprocessing.Process(
                target=self._get_line,
                args=(child_conn, peer, obj_type)
            )
            p.daemon = True
            p.start()
            it_did_timeout = (not parent_conn.poll(timeout))
            if it_did_timeout:
                logging.info("Connection to peer {} timed out.".format(peer))
                return False
            else:
                parent_said_yes = parent_conn.recv()
                if not parent_said_yes:
                    logging.info("No connection to peer {} established.".format(peer))
                return parent_said_yes
        except:
            err = sys.exc_info()
            logging.debug("NameServer encountered an error while spawning a process to check if peer {} is still alive:\n{}: {}"
                          .format(peer, err[0], err[1]))
            return False

    def _get_line(self, conn, peer, obj_type):
        result = False
        try:
            expected = [peer[0], obj_type]
            response = Stub(peer[1]).isAlive()
            result = (response == expected)
            logging.debug("NameServer received response {} from peer {}".format(response, peer))
            if result is True:
                logging.debug("This was the expected response.")
            else:
                logging.debug("This was not the expected response.\n Expected response: {}"
                              .format(expected))
        except ConnectionRefusedError:
            logging.info("Peer {} refused connection".format(peer))
            result = False
        except:
            err = sys.exc_info()
            logging.debug("NameServer encountered an error trying to check if peer {} is still alive:\n{}: {}"
                         .format(peer, err[0], err[1]))
        finally:
            conn.send(result)

# -----------------------------------------------------------------------------
# The main program
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    logging.info("NameServer listening to: {}:{}".format(server_address[0], server_address[1]))
    
    nameserver = NameServer()
    
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("", server_address[1]))
    listener.listen(1)
    
    logging.info("Press Ctrl-C to stop the name server...")
    
    try:
        while True:
            conn, addr = listener.accept()
            req = Request(nameserver, conn, addr)
            logging.debug("NameServer serving a request from {}".format(addr))
            req.start()
            logging.debug("NameServer served the request from {}".format(addr))
    except KeyboardInterrupt:
        logging.debug("NameServer manually closed.")
    finally:
        listener.close()
        logging.info("NameServer has been unbound")
