import os

def hash_md5(p_file):
    import hashlib
    hash_md5 = hashlib.md5()
    if os.path.exists(p_file):
        with open(p_file, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

    #print('md5:',hash_md5.hexdigest())
    return hash_md5.hexdigest()
