'''
Created on 21 Oct 2015

@author: Mateusz.Kasiuba
'''

from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello Worldddsdsd!'
    
if __name__ == '__main__':
    app.run()
print('important thing I tested creating a branch! :)')