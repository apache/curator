namespace java.swift org.apache.curator
namespace cpp org.apache.curator.generated
namespace java org.apache.curator.generated


enum CreateMode {
  PERSISTENT, PERSISTENT_SEQUENTIAL, EPHEMERAL, EPHEMERAL_SEQUENTIAL
}

enum CuratorEventType {
  CREATE, DELETE, EXISTS, GET_DATA, SET_DATA, CHILDREN, SYNC, GET_ACL, SET_ACL, WATCHED, CLOSING
}

enum EventType {
  None, NodeCreated, NodeDeleted, NodeDataChanged, NodeChildrenChanged
}

enum KeeperState {
  Unknown, Disconnected, NoSyncConnected, SyncConnected, AuthFailed, ConnectedReadOnly, SaslAuthenticated, Expired
}

struct CreateSpec {
  1: string path;
  2: string data;
  3: CreateMode mode;
  4: bool doAsync;
  5: bool compressed;
  6: bool creatingParentsIfNeeded;
  7: bool withProtection;
}

struct CuratorProjection {
  1: string id;
}

struct CuratorProjectionSpec {
}

struct id {
  1: string scheme;
  2: string id;
}

struct Stat {
  1: i64 czxid;
  2: i64 mzxid;
  3: i64 ctime;
  4: i64 mtime;
  5: i32 version;
  6: i32 cversion;
  7: i32 aversion;
  8: i64 ephemeralOwner;
  9: i32 dataLength;
  10: i32 numChildren;
  11: i64 pzxid;
}

struct WatchedEvent {
  1: KeeperState keeperState;
  2: EventType eventType;
  3: string path;
}

struct Acl {
  1: i32 perms;
  2: id id;
}

struct CuratorEvent {
  1: CuratorProjection projection;
  2: CuratorEventType type;
  3: i32 resultCode;
  4: string path;
  5: string context;
  6: Stat stat;
  7: binary data;
  8: string name;
  9: list<string> children;
  10: list<Acl> aclList;
  11: WatchedEvent watchedEvent;
}

service CuratorService {
  void closeCuratorProjection(1: CuratorProjection projection);
  string create(1: CuratorProjection projection, 2: CreateSpec createSpec);
  CuratorEvent getNextEvent();
  CuratorProjection newCuratorProjection(1: CuratorProjectionSpec spec);
}
