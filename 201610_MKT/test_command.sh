import json
a = []
z = {'interface': '1/2/1', 'InMcastPkts': '917980', 'InBcastPkts': '0'}
a.append(z.copy())
z = {'interface': '1/2/2', 'InMcastPkts': '22969', 'InBcastPkts': '0'}
a.append(z.copy())
z = {'interface': '1/2/3', 'InMcastPkts': '965990', 'InBcastPkts': '1386376'}
a.append(z.copy())
z = {'interface': '1/2/4', 'InMcastPkts': '758054', 'InBcastPkts': '0'}
a.append(z.copy())
z = {'interface': '2/1/3', 'InMcastPkts': '161161', 'InBcastPkts': '0'}
a.append(z.copy())
z = {'interface': '2/2/1', 'InMcastPkts': '849182', 'InBcastPkts': '691291'}
a.append(z.copy())
z = {'interface': '2/2/2', 'InMcastPkts': '161474', 'InBcastPkts': '695084'}
a.append(z.copy())
z = {'interface': '2/2/3', 'InMcastPkts': '759274', 'InBcastPkts': '0'}
a.append(z.copy())

print ("a:",a,"\n")

b = json.dumps(a)
print ( "b:",b,"\n")

c = json.loads(b)
print ("c:",c,"\n")

print ("\n")
print (type(a),"\n",type(b),"\n",type(c))

print ("a[0]:",a[0])
print ("c[0]:",c[0])
print ("c[0]['interface']:",c[0]['interface'] )
print ("c[0]['InMcastPkts']:",c[0]['InMcastPkts'] )
print ("c[0]['InMcastPkts']:",int(c[0]['InMcastPkts'])*2 )
