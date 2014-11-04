# coding: utf-8

import sys
sys.path.append('/home/plum/kimo_emo')
from Tokenizer import Tokenizer

import pymongo
db = pymongo.Connection('localhost')['ptt']
co = db['HatePolitics']

tkzr = Tokenizer()

log = open('tokenize_error_fn.log', 'w')

fns = [x['fn'] for x in co.find()]

for i, fn in enumerate(fns):

    mdoc = co.find_one({'fn':fn})

    if 'CKIP' in mdoc: 
        print '>',i,'skip', fn
        continue

    print '>',i,'processing', fn, '...',
    sys.stdout.flush()
    
    raw = mdoc['ugBody'].encode('utf-8')
    try:
        tokenized = tkzr.tokenizeStr(raw).decode('utf-8')
    except:
        print 'ERROR'
        log.write(fn + '\n')
        continue
    
    co.update( {'fn': fn}, { '$set': {'CKIP': tokenized } } )
    print 'OK'
    
