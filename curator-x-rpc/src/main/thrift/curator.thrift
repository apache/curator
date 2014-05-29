namespace java.swift org.apache.curator
namespace cpp org.apache.curator.generated
namespace java org.apache.curator.generated


enum CreateMode {
  PERSISTENT, PERSISTENT_SEQUENTIAL, EPHEMERAL, EPHEMERAL_SEQUENTIAL
}

enum PathChildrenCacheStartMode {
  NORMAL, BUILD_INITIAL_CACHE, POST_INITIALIZED_EVENT
}

enum CuratorEventType {
  PING, CREATE, DELETE, EXISTS, GET_DATA, SET_DATA, CHILDREN, SYNC, GET_ACL, SET_ACL, WATCHED, CLOSING, CONNECTION_CONNECTED, CONNECTION_SUSPENDED, CONNECTION_RECONNECTED, CONNECTION_LOST, CONNECTION_READ_ONLY, LEADER
}

enum EventType {
  None, NodeCreated, NodeDeleted, NodeDataChanged, NodeChildrenChanged
}

enum KeeperState {
  Unknown, Disconnected, NoSyncConnected, SyncConnected, AuthFailed, ConnectedReadOnly, SaslAuthenticated, Expired
}

struct CreateSpec {
  1: string path;
  2: binary data;
  3: CreateMode mode;
  4: string asyncContext;
  5: bool compressed;
  6: bool creatingParentsIfNeeded;
  7: bool withProtection;
}

struct CuratorProjection {
  1: string id;
}

struct ExistsSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
}

struct GenericProjection {
  1: string id;
}

struct GetChildrenSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
}

struct GetDataSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
  4: bool decompressed;
}

struct LeaderProjection {
  1: GenericProjection projection;
}

struct PathChildrenCacheProjection {
  1: GenericProjection projection;
}

struct Version {
  1: i32 version;
}

struct LeaderEvent {
  1: string path;
  2: string participantId;
  3: bool isLeader;
}

struct LeaderResult {
  1: LeaderProjection projection;
  2: bool hasLeadership;
}

struct OptionalChildrenList {
  1: list<string> children;
}

struct OptionalPath {
  1: string path;
}

struct Id {
  1: string scheme;
  2: string id;
}

struct Participant {
  1: string id;
  2: bool isLeader;
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

struct DeleteSpec {
  1: string path;
  2: bool guaranteed;
  3: string asyncContext;
  4: bool compressed;
  5: Version version;
}

struct SetDataSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
  4: bool compressed;
  5: Version version;
  6: binary data;
}

struct OptionalStat {
  1: Stat stat;
}

struct Acl {
  1: i32 perms;
  2: Id id;
}

struct CuratorEvent {
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
  12: LeaderEvent leaderEvent;
}

service CuratorService {
  GenericProjection acquireLock(1: CuratorProjection projection, 2: string path, 3: i32 maxWaitMs);
  void closeCuratorProjection(1: CuratorProjection projection);
  bool closeGenericProjection(1: CuratorProjection curatorProjection, 2: GenericProjection genericProjection);
  OptionalPath createNode(1: CuratorProjection projection, 2: CreateSpec spec);
  void deleteNode(1: CuratorProjection projection, 2: DeleteSpec spec);
  OptionalStat exists(1: CuratorProjection projection, 2: ExistsSpec spec);
  OptionalChildrenList getChildren(1: CuratorProjection projection, 2: GetChildrenSpec spec);
  binary getData(1: CuratorProjection projection, 2: GetDataSpec spec);
  list<Participant> getLeaderParticipants(1: CuratorProjection projection, 2: LeaderProjection leaderProjection);
  bool isLeader(1: CuratorProjection projection, 2: LeaderProjection leaderProjection);
  CuratorProjection newCuratorProjection(1: string connectionName);
  Stat setData(1: CuratorProjection projection, 2: SetDataSpec spec);
  LeaderResult startLeaderSelector(1: CuratorProjection projection, 2: string path, 3: string participantId, 4: i32 waitForLeadershipMs);
  PathChildrenCacheProjection startPathChildrenCache(1: CuratorProjection projection, 2: string path, 3: bool cacheData, 4: bool dataIsCompressed, 5: PathChildrenCacheStartMode startMode);
}

service EventService {
  CuratorEvent getNextEvent(1: CuratorProjection projection);
}
