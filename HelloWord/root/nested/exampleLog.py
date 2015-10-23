'''
Created on 21 Oct 2015

@author: Mateusz.Kasiuba
'''

from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    
    print('trying logger info')
    import logging
    logging.basicConfig(filename='example.log',level=logging.DEBUG)
    logging.debug('This message should go to the log file')
    logging.info('So should this')
    logging.warning('And this, too')

    print('important thing I tested creating a branch! :) example')
    return 'Hello Worldddsdsd!'
    
if __name__ == '__main__':
    app.run()
