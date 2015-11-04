
import os, requests, json, uuid, shutil

from flask import Flask, request, send_file, abort, make_response, logging

from logging import Formatter
from logging.handlers import RotatingFileHandler


app = Flask(__name__)
app.config['HOST'] = "localhost"
app.config['DEBUG'] = True
app.config['PORT'] = 5999
app.config['STORAGE_FOLDER'] = '/home/mark/store'

file_handler = RotatingFileHandler('/home/mark/jperstorelog', maxBytes=1000000000, backupCount=5)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(Formatter(
    '%(asctime)s %(levelname)s: %(message)s '
    '[in %(pathname)s:%(lineno)d %(module)s %(funcName)s]'
))
app.logger.addHandler(file_handler)


@app.route('/')
@app.route('/<path:path>', methods=['GET','POST','PUT','DELETE'])
def storage(path=''):
    if '..' in path or path.startswith('/'):
        abort(400)
    dir = app.config['STORAGE_FOLDER'] + '/' + path
    if path == '':
        listing = os.listdir( dir )
        resp = make_response( json.dumps( listing ) )
        resp.mimetype = "application/json"
        return resp
    elif request.method == 'DELETE' or ( request.method == 'POST' and request.form.get('submit','').lower() == 'delete' ):
        if not os.path.exists(dir):
            abort(404)
        elif os.path.isfile(dir):
            os.remove(dir)
        else:
            shutil.rmtree(dir)
        return ''
    elif request.method == 'PUT' and not os.path.exists(dir):
        os.makedirs(dir)
        return ''
    elif request.method == 'POST':
        sdir = dir.rstrip('/').rsplit('/',1)[0]
        if not os.path.exists(sdir):
            os.makedirs(sdir)
        try:
            file = request.files['file']
            file.save(dir)
            return ''
        except:
            data = json.dumps( request.json if request.json else request.values )
            if len(data) > 0:
                try:
                    out = open(dir, 'w')
                    out.write(data)
                    out.close()
                    return ''
                except:
                    abort(400)
            else:
                abort(400)
    elif request.method == 'GET':
        if not os.path.exists(dir):
            abort(404)
        elif os.path.isfile(dir):
            return send_file(dir)
        else:
            listing = os.listdir( dir )
            resp = make_response( json.dumps( listing ) )
            resp.mimetype = "application/json"
            return resp
    else:
        abort(400)


if __name__ == "__main__":
    if not os.path.exists(app.config['STORAGE_FOLDER']):
        print 'Storage folder does not exist!'
        exit
    else:
        app.run(host='0.0.0.0', debug=app.config['DEBUG'], port=app.config['PORT'])

