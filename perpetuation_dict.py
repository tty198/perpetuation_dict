import pickle
import os
import json
import shutil
from typing import TypeVar
from tqdm.auto import tqdm
from pickle import UnpicklingError

TV = TypeVar('TV')
class perpetuation_dict(dict[str, TV]):
    __ext = '.pddat'
    __bak = '.pdbak'
    __amp = '.amp'
    __ampbak = '.ampbak'
    def __init__(self) -> None:
        self.path :str
        self.index :dict[str, tuple[int, int]] = {}
        self.cache :dict[str, TV] = {}
    
    def __getitem__(self, key:str)->TV|None:
        try:
            if key in self.cache:
                return self.cache[key]
            else:
                if key in self.index:
                    start, size = self.index[key]
                    ba = self.__load(start, size)
                    value = pickle.loads(ba)
                    self.cache[key] = value
                    return value
                else:
                    raise ValueError(f"key not found key: '{key}'")
        except UnpicklingError as upe:
            print(f"key:{key}  \nerror:{upe}")
            return None

    def __setitem__(self, key:str, value:TV):
        self.cache[key] = value
        
    def __len__(self)->int:
        return len(self.index)
    
    def __eq__(self, other)->bool:
        return other in self.index or other in self.cache
    
    def __contains__(self, key:str)->bool:
        return key in self.cache or key in self.index

    def __delitem__(self, key):
        try:
            del self.index[key]
        except KeyError:
            pass
        try:
            del self.cache[key]
        except KeyError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def __iter__(self):
        for k in self.index.keys():
            yield k
    
    def __save(self):
        #print('save')
        source = self.path + perpetuation_dict.__ext
        backup = self.path + perpetuation_dict.__bak
        amp_source = self.path + perpetuation_dict.__amp
        amp_backup = self.path + perpetuation_dict.__ampbak
        
        if os.path.exists(source) and os.path.exists(amp_source):
            try:
                shutil.copy2(source, backup)
                shutil.copy2(amp_source, amp_backup)
                os.remove(source)
                os.remove(amp_source)
                self.__write(source, amp_source)
            except OSError as ose:
                os.rename(backup, source)
                os.rename(amp_backup, amp_source)
                raise Exception('File cant save exception', ose)
            except Exception as e:
                os.rename(backup, source)
                os.rename(amp_backup, amp_source)
                raise Exception('File cant save exception', e)
            else:
                os.remove(backup)
                os.remove(amp_backup)
                self.cache.clear()
        else:
            try:
                directory = os.path.dirname(source)
                if not os.path.exists(directory):
                    os.makedirs(directory)
                elif os.path.exists(source):
                    os.remove(source)
                self.__write(source, amp_source)
            except Exception as e:
                raise Exception('File cant save exception', e)
            else:
                self.cache.clear()

    def __write(self, source, amp_source):
        self.index = {}
        addr = 0
        with open(source, 'ab') as f:
            for k, v in tqdm(self.cache.items(), desc='[writing]', leave=False):
                binary = pickle.dumps(v)
                size = len(binary)
                f.write(binary)
                self.index[k] = (addr, size)
                addr += size
                        
        with open(amp_source, 'w') as f:
            json.dump(self.index, f)

    @classmethod
    def open(cls, path:str)-> 'perpetuation_dict[TV]':
        """path is filepath + filename"""
        c = cls()
        c.path = path
        if os.path.exists(path + perpetuation_dict.__ext) and os.path.exists(path + perpetuation_dict.__amp):
            with open(path + perpetuation_dict.__amp, 'r') as f:
                c.index = json.load(f)
        return c
    
    def __load(self, s=0, e=0):
        with open(self.path + perpetuation_dict.__ext, 'rb') as f:
            f.seek(s, 0)
            return f.read(e)
            
    def sync(self):
        """synchronises, cache is cleared."""
        currentdict = {}
        key, val, da = None, None, bytes()
        try:
            for k, v in tqdm(self.index.items(), desc='[sync]', leave=False):
                """load only for updates"""
                if k in self.cache:
                    continue
                data = self.__load(v[0], v[1])
                key, val, da = k, v, data
                currentdict[k] = pickle.loads(data)
        except EOFError as eof:
            print(f'key:{key} val:{val} datalen: {len(da)}\n' ,eof)
        self.cache = currentdict | self.cache
        self.__save()
    
    def close(self):
        self.sync()
        self.cache.clear()
        del self.cache
            
    def update(self, dic:dict):
        """update from dict"""
        self.cache.update(dic)
        
    def clear(self):
        """clear cache and unwritten data will disappear"""
        self.cache.clear()
        
    def fullcache(self):
        """cache all values"""
        for k in tqdm(self.index.keys(), desc='[cache]', leave=False):
            self[k]
    
    def renew(self):
        """clear index and cache"""
        self.cache.clear()
        self.index.clear()

if __name__ == '__main__':
    if False:
        import random
        testdir = r"C:\test\test"
        perpd :perpetuation_dict[int] = perpetuation_dict.open(testdir)
        perpd.renew()
        
        testkeys = []
        random.seed(2023)
        randomdata = [random.randint(0, 50000000) for i in range(1000000)]
        
        for i, v in enumerate(randomdata):
            k = str(random.randint(0, 5000000))
            perpd[k] = v
            if i % 10000 == 0:
                testkeys.append(k)

        perpd.sync()
        perpd.close()
        del perpd
    
        perpd = perpetuation_dict.open(testdir)
        for key in testkeys:
            print(perpd[key])