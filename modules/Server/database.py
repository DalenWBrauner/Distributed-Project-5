# -----------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------

"""Implementation of a simple database class."""
import random

class Database(object):
    """Class containing a database implementation."""

    def __init__(self, db_filename):
        #self.db_file = db_file         # No, this is not the file
        self.db_filename = db_filename 
        self.rand = random.Random()
        self.rand.seed()

    def read(self):
        """Read a random location in the database."""
        db_file = open(self.db_filename,'r').read()
        db_text = db_file.read()
        db_file.close()
	
        fortune_list = db_text.split("\n%\n")
        rand_fortune = self.rand.randint(0,len(fortune_list))
        return fortune_list[rand_fortune] + "\n"

    def write(self, fortune):
        """Write a new fortune to the database."""
        if '\\n' in fortune:    # Translate newline characters correctly
            splitfortune = fortune.split('\\n')
            fortune = "\n".join(splitfortune)
        if '\\t' in fortune:    # Translate tab characters correctly
            splitfortune = fortune.split('\\t')
            fortune = "\t".join(splitfortune)

        # Write the fortune to the database
        db_file = open(self.db_filename,'a')
        db_file.write(fortune + "\n%\n") #We assume there is always one at the end
        db_file.close()
