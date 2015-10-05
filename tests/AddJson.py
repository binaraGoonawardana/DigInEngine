__author__ = 'Marlon'
import json

json1 = '{ "fruit": [ { "name": "apple", "color": "red" }, { "name": "orange", "color": "orange" } ] }'

json2 = '{ "fruit": [ { "name": "strawberry", "color": "red", "size": "small" }, { "name": "orange", "size": "medium" } ] }'

dicta = json.loads(json1)
print dicta
dictb = json.loads(json2)

def merge(lsta, lstb):
    for i in lstb:
        for j in lsta:
            if j['name'] == i['name']:
                j.update(i)
                break
        else:
            lsta.append(i)

for k,v in dictb.items():
    print 'k: ' + str(k)
    print 'v: ' + str(v)
    merge(dicta.setdefault(k, []), v)


if  __name__ == "__main__":
    merge(dicta, dictb)
