#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <deque>

#include "include/org_rocksdb_RocksDB.h"
#include "include/org_rocksdb_WriteBatch.h"
#include "include/org_rocksdb_RocksIterator.h"
#include "include/org_rocksdb_Weez.h"

#include "rocksdb/merge_operator.h"
#include "rocksjni/portal.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"

struct WeezBufResult{
  jlong handle;
  jlong ptr;
  jint len;
  jint reserved;
  inline void tryFreeHandle(){
    if (handle){
      delete reinterpret_cast<std::string*>(handle);
      handle = 0;
    }
  }
  inline void setStr(std::string * str){
    handle = reinterpret_cast<jlong>(str);
    ptr = reinterpret_cast<jlong>(str->data());
    len = (jint)(str->size());
  }
  inline void setSlice(rocksdb::Slice slice){
    ptr = reinterpret_cast<jlong>(slice.data());
    len = (jint)(slice.size());
  }
};


jboolean Java_org_rocksdb_Weez_riSeek0(
    JNIEnv* env, jclass jclazz, jlong handle,
    jlong jkeyMin,jint jkeyMinLen,jboolean excludeMin,
    jlong jkeyMax,jint jkeyMaxLen,jboolean excludeMax,
    jboolean backward,jlong jkeyResult,jlong jvalueResult) {
  auto it = reinterpret_cast<rocksdb::Iterator*>(handle);
  auto keyMin = rocksdb::Slice(reinterpret_cast<char*>(jkeyMin), jkeyMinLen);
  auto keyMax = rocksdb::Slice(reinterpret_cast<char*>(jkeyMax), jkeyMaxLen);
  auto kResult = reinterpret_cast<WeezBufResult*>(jkeyResult);
  auto vResult = reinterpret_cast<WeezBufResult*>(jvalueResult);
  kResult->tryFreeHandle();
  if (vResult != nullptr){
    vResult->tryFreeHandle();
  }

  rocksdb::Slice newKey;
  if (backward){
    if (jkeyMaxLen==0){
      it->SeekToLast();
    }else{
      it->Seek(keyMax);
    }
    if (it->Valid()){
      newKey = it->key();
      if((jkeyMaxLen>0) && (excludeMax || (newKey.compare(keyMax)>0))){
        it->Prev();
        if (!it->Valid()){
          return false;
        }
        newKey = it->key();
      }
    }else if (jkeyMaxLen==0){
      return false;
    }else{
      it->SeekToLast();
      if (!it->Valid()){
        return false;
      }
      newKey = it->key();
    }
    if (jkeyMinLen>0){
      auto c = newKey.compare(keyMin);
      if (c<0 || ((c==0) && excludeMin)){
        return false;
      }
    }
  }else{
    if (jkeyMinLen==0){
      it->SeekToFirst();
    }else{
      it->Seek(keyMin);
    }
    if (!it->Valid()){
      return false;
    }
    newKey = it->key();
    if (excludeMin && (jkeyMinLen>0) && (newKey.compare(keyMin)==0)){
      it->Next();
      if (!it->Valid()){
        return false;
      }
      newKey = it->key();
    }
    if (jkeyMaxLen>0){
      auto c = newKey.compare(keyMax);
      if (c>0 || ((c==0) && excludeMax)){
        return false;
      }
    }
  }
  kResult->setSlice(newKey);
  if (vResult){
    vResult->setSlice(it->value());
  }
  return true;
}
void Java_org_rocksdb_Weez_riValue0(
    JNIEnv* env, jclass jclazz, jlong handle,
    jlong jvalueResult) {
  auto it = reinterpret_cast<rocksdb::Iterator*>(handle);
  auto vResult = reinterpret_cast<WeezBufResult*>(jvalueResult);
  vResult->tryFreeHandle();
  vResult->setSlice(it->value());
}
jboolean Java_org_rocksdb_Weez_riStep0(
    JNIEnv* env, jclass jclazz, jlong handle,
    jint step,jlong juntilKey,jint juntilKeyLen,jboolean excludeUntilKey,
    jlong jkeyResult,jlong jvalueResult) {
  auto it = reinterpret_cast<rocksdb::Iterator*>(handle);
  auto untilKey = rocksdb::Slice(reinterpret_cast<char*>(juntilKey), juntilKeyLen);
  auto kResult = reinterpret_cast<WeezBufResult*>(jkeyResult);
  auto vResult = reinterpret_cast<WeezBufResult*>(jvalueResult);
  kResult->tryFreeHandle();
  if (vResult){
    vResult->tryFreeHandle();
  }
  bool backward;
  if (step>0){
    do{
      it->Next();
      if (!it->Valid()){
        return false;
      }
      step--;
    }while(step>0);
    backward = false;
  }else if (step<0){
    do{
      it->Prev();
      if (!it->Valid()){
        return false;
      }
      step++;
    }while(step<0);
    backward = true;
  }else if(!it->Valid()){
    return false;
  }
  auto newKey = it->key();
  if (juntilKeyLen>0){
     auto c = newKey.compare(untilKey);
     if (c==0){
       if (excludeUntilKey){
         return false;
       }
     }else if ((c>0)^backward){
       return false;
     }
  }
  kResult->setSlice(newKey);
  if (vResult){
    vResult->setSlice(it->value());
  }
  return true;
}

const int ENTRY_TYPE_SMALL = 0b0010;
const int ENTRY_TYPE_SUMS = 0b0001;
const int SUM_TYPE_0 = 5;
const int SUM_TYPE_BYTE = 11;
const int SUM_TYPE_SHORT = 12;
const int SUM_TYPE_INT = 13;
const int SUM_TYPE_INT_48 = 14;
const int SUM_TYPE_LONG = 15;

class WeezSumsReader{
 public:
  WeezSumsReader(const rocksdb::Slice &value){
    ptr = value.data();
    int h = *ptr & 0xFF;
    ptr++;
    if ((h & ENTRY_TYPE_SMALL)==0){
      h |= *ptr << 8;
      ptr++;
    }
    _fieldCount = (h>>4) + 1;
    types = 1;
  }
  inline int fieldCount(){
    return _fieldCount;
  }
  inline jlong read(){
    if (types==1){
      types = 0x100 | *ptr;
      ptr++;
    }
    int type = types & 0xF;
    types>>=4;
    const char * p = ptr;
    switch(type){
      case SUM_TYPE_BYTE:
        ptr++;
        return *((jbyte*)p);
      case SUM_TYPE_SHORT:
        ptr+=2;
        return *((jshort*)p);
      case SUM_TYPE_INT:
        ptr+=4;
        return *((jint*)p);
      case SUM_TYPE_INT_48:
        ptr+=6;
        return ((*((jlong*)p))<<16)>>16;
      case SUM_TYPE_LONG:
        ptr+=8;
        return *((jlong*)p);
      default:
        return type - SUM_TYPE_0;
    }
  }
 private:
  int types;
  const char* ptr;
  int _fieldCount;
};

const int MIN_TINY_SUM = -SUM_TYPE_0;
const int MAX_TINY_SUM = SUM_TYPE_BYTE - SUM_TYPE_0 - 1;
const int MIN_BYTE = -0x80;
const int MAX_BYTE = 0x7F;
const int MIN_SHORT = -0x8000;
const int MAX_SHORT = 0x7FFF;
const int MIN_INT = -0x80000000;
const int MAX_INT = 0x7FFFFFFF;
const jlong MIN_INT_48 = 0xFFFF800000000000L;
const jlong MAX_INT_48 = 0x00007FFFFFFFFFFFL;

class WeezSumsWriter{
 public:
  WeezSumsWriter(std::string* new_value,int fieldCount){
    rep = new_value;
    new_value->clear();
    new_value->reserve((fieldCount <= 0x100 ? 1 : 2) + fieldCount * 8 + (fieldCount + 1) / 2);
    int h = ENTRY_TYPE_SUMS;
    fieldCount--;
    h |= (fieldCount<<4)& 0xF0;
    if (fieldCount>0x100){
      h |= (fieldCount>>4)<<8;
      new_value->append((char *)&h,2);
    }else{
      h |= ENTRY_TYPE_SMALL;
      new_value->append((char *)&h,1);
    }
    types = 0;
  }
  inline void write(jlong sum){
    if (MIN_INT <= sum && sum <= MAX_INT) {
        jint v = (jint) sum;
        if (MIN_BYTE <= sum && sum <= MAX_BYTE) {
            if (MIN_TINY_SUM <= v && v <= MAX_TINY_SUM) {
                writeType(v - SUM_TYPE_0);
            } else {
                writeType(SUM_TYPE_BYTE);
                rep->append((char*)&v,1);
            }
        } else if (MIN_SHORT <= v && v <= MAX_SHORT) {
            writeType(SUM_TYPE_SHORT);
            rep->append((char*)&v,2);
        } else {
            writeType(SUM_TYPE_INT);
            rep->append((char*)&v,4);
        }
    } else if (MIN_INT_48 <= sum && sum <= MAX_INT_48) {
        writeType(SUM_TYPE_INT_48);
        rep->append((char*)&sum,6);
    } else {
        writeType(SUM_TYPE_LONG);
        rep->append((char*)&sum,8);
    }
  }
 private:
  inline void writeType(int type){
    if (typePos == 0){
      (*rep) +=(char)type;
      typePos = rep->size()-1;
    }else{
      (*rep)[typePos] |= (char)(type<<4);
      typePos = 0;
    }
  }
  size_t typePos=0;
  int types;
  std::string * rep;
};

class WeezSumOperator : public rocksdb::AssociativeMergeOperator {
 public:
  bool Merge(const rocksdb::Slice& key,
                      const rocksdb::Slice* existing_value,
                      const rocksdb::Slice& value,
                      std::string* new_value,
                      rocksdb::Logger* logger) const override{
      assert(new_value);
      new_value->clear();
      if (!existing_value) {
        new_value->assign(value.data(),value.size());
      } else {
        WeezSumsReader reader0(*existing_value);
        WeezSumsReader reader1(value);
        int l0 = reader0.fieldCount();
        int l1 = reader1.fieldCount();
        assert(l1>=l0);
        WeezSumsWriter writer(new_value,l1);
        for(int i=0;i<l0;i++){
          writer.write(reader0.read()+reader1.read());
        }
        for(int i=l0;i<l1;i++){
          writer.write(reader1.read());
        }
      }
    return true;
  }
  const char* Name() const  {
    return "WeezSumOperator";
  }
};

jlong Java_org_rocksdb_Weez_newWeezSumOperatorHandle(JNIEnv* env, jclass jclazz) {
  std::shared_ptr<rocksdb::MergeOperator> *op = new std::shared_ptr<rocksdb::MergeOperator>();
  *op = std::make_shared<WeezSumOperator>();
  return reinterpret_cast<jlong>(op);
}

jboolean Java_org_rocksdb_Weez_dbGet0(
    JNIEnv* env, jclass jclazz, jlong jdb_handle,jlong jropt_handle,jlong jcf_handle,
    jlong jkey,jint jkey_len,jlong jvalue_result) {
  auto * valueResult = reinterpret_cast<WeezBufResult*>(jvalue_result);
  valueResult->tryFreeHandle();
  auto * db = reinterpret_cast<rocksdb::DB*>(jdb_handle);
  auto & read_opt = *reinterpret_cast<rocksdb::ReadOptions*>(jropt_handle);
  rocksdb::Slice key_slice(reinterpret_cast<char*>(jkey), jkey_len);
  auto cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  auto * valuePtr = new std::string;
  rocksdb::Status s;
  if (cf_handle != nullptr) {
      s = db->Get(read_opt, cf_handle, key_slice, valuePtr);
  } else {
      s = db->Get(read_opt, key_slice, valuePtr);
  }
  if (s.IsNotFound()) {
    delete valuePtr;
    return false;
  }
  if (s.ok()) {
    valueResult->setStr(valuePtr);
    return true;
  }
  delete valuePtr;
  rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
  return false;
}

void Java_org_rocksdb_Weez_freeBufResult(JNIEnv* env, jclass jclazz,jlong jbuf_result,jint size) {
  auto * bufResult = reinterpret_cast<WeezBufResult*>(jbuf_result);
  for(int i=0;i<size;i++){
    bufResult[i].tryFreeHandle();
  }
}


inline jint getOffset(char* ptr){
  return (*((jint*)ptr))& 0xFFFFFF;
}

const jint SUMS_KIND_ADD = 0x8000;
const jint SUMS_KIND_PUT = 0x4000;
const jint SUMS_KIND_DEL = SUMS_KIND_ADD | SUMS_KIND_PUT;
const jint SUMS_KIND_MASK = SUMS_KIND_ADD | SUMS_KIND_PUT;

inline jint getKeyLen(char* ptr,jint &sumsKind){
  jshort kh = (*((jshort*)ptr));
  sumsKind = (kh & SUMS_KIND_MASK) != 0;
  return kh & 0x0FFF;
}

inline char* getAddress(char* blockPtr,jint offset){
  if (offset==0){
    return nullptr;
  }
  return blockPtr + offset;
}

inline void getValueChunkHeadInfo(char* ptr,char* blockPtr,int &used,char* &nextPtr){
    used = (*((jshort*)(ptr+2))) & 0xFFFF;
    nextPtr = getAddress(blockPtr,getOffset(ptr+4));
}

jint Java_org_rocksdb_Weez_wbPutAll0(
    JNIEnv* env, jclass jclazz,jlong jhandle,jlong jcf_handle,jlong block,
    jlong first) {
  auto* wb = reinterpret_cast<rocksdb::WriteBatch*>(jhandle);
  auto* cf_handle = reinterpret_cast<rocksdb::ColumnFamilyHandle*>(jcf_handle);
  char* entryPtr = reinterpret_cast<char*>(first);
  char* blockPtr = reinterpret_cast<char*>(block);
  assert(wb != nullptr);
  jint count=0;
  while(entryPtr != nullptr){
    count++;
    jint sumsKind;
    int len = getKeyLen(entryPtr+1,sumsKind);
    char* nextPtr = getAddress(blockPtr,getOffset(entryPtr+6));
    entryPtr += 9 + (*entryPtr) * 3;
    rocksdb::Slice key_slice(entryPtr, len);
    entryPtr += len;
    char* nextChunkPtr;
    getValueChunkHeadInfo(entryPtr,blockPtr,len,nextChunkPtr);
    if ((len==0 && nextChunkPtr==nullptr) || sumsKind == SUMS_KIND_DEL){//del
      wb->Delete(cf_handle,key_slice);
    }else if (nextChunkPtr==nullptr || sumsKind == SUMS_KIND_PUT){
      if (len==0){//sums
        entryPtr = nextChunkPtr;
        getValueChunkHeadInfo(entryPtr,blockPtr,len,nextChunkPtr);
      }
      wb->Put(cf_handle,key_slice,rocksdb::Slice(entryPtr+7, len));
    }else if (sumsKind==SUMS_KIND_ADD){
      if (len==0){
        entryPtr = nextChunkPtr;
        getValueChunkHeadInfo(entryPtr,blockPtr,len,nextChunkPtr);
      }
      wb->Merge(cf_handle,key_slice,rocksdb::Slice(entryPtr+7, len));
    }else{//not one chunk
      int num_slice = len ? 1 : 0;
      char * ptr = nextChunkPtr;
      while(ptr != nullptr){
        int l;
        getValueChunkHeadInfo(ptr,blockPtr,l,ptr);
        if (l){
          num_slice++;
        }
      }
      rocksdb::Slice* slices = new rocksdb::Slice[num_slice];
      num_slice=0;
      for(;;){
          if (len){
            slices[num_slice++] = rocksdb::Slice(entryPtr+7,len);
          }
          entryPtr = nextChunkPtr;
          if (nextChunkPtr != nullptr){
            entryPtr = nextChunkPtr;
            getValueChunkHeadInfo(entryPtr,blockPtr,len,nextChunkPtr);
          }else{
            break;
          }
      }
      wb->Put(cf_handle,rocksdb::SliceParts(&key_slice, 1),rocksdb::SliceParts(slices, num_slice));
      delete slices;
    }
    entryPtr = nextPtr;
  }
  return count;
}