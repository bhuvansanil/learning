import os
import json
from flask import request, make_response, jsonify, abort
with os.scandir('/home/python/digi_images') as entries:
    output = []

    for entry in entries:
        size = os.path.getsize('/home/python/digi_images/' + entry.name)
        x = {'name':entry.name,'size':size}
        output.append(x)
    print(json.dumps(output))
       # print(jsonify(x))