//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/lock_stl_db.h"
#include "db/rocks_db.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props, bool preloaded) {
  if (props["dbname"] == "basic") {
    return new BasicDB(props);
  } else if (props["dbname"] == "lock_stl") {
    assert(!preloaded);
    return new LockStlDB;
  } else if (props["dbname"] == "rocksdb") {
    return new RocksDB(props, preloaded);
  } else return NULL;
}

