namespace java.swift org.apache.curator.x.rpc.idl.projection
namespace cpp org.apache.curator


enum CreateMode {
  PERSISTENT, PERSISTENT_SEQUENTIAL, EPHEMERAL, EPHEMERAL_SEQUENTIAL
}

struct CreateSpec {
  1: string path;
  2: string asyncId;
  3: string data;
  4: CreateMode mode;
  5: bool async;
  6: bool compressed;
  7: bool creatingParentsIfNeeded;
  8: bool withProtection;
}

struct CuratorProjection {
  1: string id;
}

struct CuratorProjectionSpec {
}

service CuratorService {
  void closeCuratorProjection(1: CuratorProjection projection);
  string create(1: CuratorProjection projection, 2: CreateSpec createSpec);
  CuratorProjection newCuratorProjection(1: CuratorProjectionSpec spec);
}
